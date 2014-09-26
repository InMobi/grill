package com.inmobi.grill.server.scheduler;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.metastore.CubeMetastoreService;
import com.inmobi.grill.server.api.query.QueryExecutionService;
import com.inmobi.grill.server.api.scheduler.Execution;
import com.inmobi.grill.server.api.scheduler.GrillScheduleRunInfo;
import com.inmobi.grill.server.session.HiveSessionService;

public class ScheduleQueryExecution implements Execution {
  private Gson gson = new Gson();
  private static final Log LOG = LogFactory
      .getLog(ScheduleQueryExecution.class);
  private QueryExecutionService executionService;
  private HiveSessionService sessionService;
  private GrillSessionHandle sessionHandle;
  private CubeMetastoreService metastoreService;
  private QueryHandle queryHandle;
  private Date start;
  private Date end;
  private GrillSchedulerDAO grillSchedulerDAO;
  private Configuration conf;

  public ScheduleQueryExecution() {
    conf = new Configuration();
  }

  protected QueryExecutionService getQuerySvc() {
    if (executionService == null) {
      executionService =
          (QueryExecutionService) GrillServices.get().getService(
              QueryExecutionService.NAME);
    }
    if (executionService == null) {
      throw new NullPointerException(
          "Could not get QueryExecutionService service");
    }
    return executionService;
  }

  protected CubeMetastoreService getMetaStoreSvc() {
    if (metastoreService == null) {
      metastoreService =
          (CubeMetastoreService) GrillServices.get().getService(
              CubeMetastoreService.NAME);
    }
    if (metastoreService == null) {
      throw new NullPointerException(
          "Could not get CubeMetastoreService service");
    }
    return metastoreService;
  }

  protected HiveSessionService getSessionSvc() {
    if (sessionService == null) {
      sessionService =
          (HiveSessionService) GrillServices.get().getService(
              HiveSessionService.NAME);
    }
    if (sessionService == null) {
      throw new NullPointerException("Could not get HiveSessionService service");
    }
    return sessionService;
  }

  @Override
  public String getId() throws GrillException {
    return queryHandle.getHandleId().toString();
  }

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    start = new Date();
    JobDataMap dataMap = context.getJobDetail().getJobDataMap();

    String query = dataMap.getString("query");
    String scheduleid = dataMap.getString("scheduleid");
    String session_db = dataMap.getString("session_db");

    String qConf = dataMap.getString("query_conf");
    GrillConf queryConf = gson.fromJson(qConf, GrillConf.class);

    String schConf = dataMap.getString("schedule_conf");
    GrillConf scheduleConf = gson.fromJson(schConf, GrillConf.class);

    TypeToken<List<String>> list = new TypeToken<List<String>>() {
    };
    List<String> resource_path =
        gson.fromJson(dataMap.getString("resource_path"), list.getType());

    TypeToken<HashMap<String, String>> stringStringMap =
        new TypeToken<HashMap<String, String>>() {
        };
    HashMap<String, String> sessionConf =
        gson.fromJson(dataMap.getString("session_conf"),
            stringStringMap.getType());

    try {
      sessionHandle = sessionService.openSession("", "", sessionConf);
      metastoreService.setCurrentDatabase(sessionHandle, session_db);
    } catch (GrillException e) {
      LOG.error("Exception occured while creating session for schedule query.",
          e);
    }

    for (String path : resource_path) {
      sessionService.addResource(sessionHandle, "jar/file", path);
    }

    QueryStatus status = new QueryStatus(0, Status.NEW, "", false, "", "");
    try {
      queryHandle =
          executionService.executeAsync(sessionHandle, query, queryConf,
              "Schedule Query: " + scheduleid);
      while (status.isFinished()) {
        try {
          Thread.sleep(Integer.parseInt(scheduleConf.getProperties().get(
              "grill.scheduler.poll.interval")));
        } catch (InterruptedException e) {
          LOG.error("Error while polling for status.", e);
        }
        status =
            executionService.getQuery(sessionHandle, queryHandle).getStatus();
      }
      end = new Date();
      updateRunInfo(scheduleid, queryHandle, sessionHandle, status.getStatus(),
          start, end, executionService.getQuery(sessionHandle, queryHandle)
              .getResultSetPath());
    } catch (GrillException e) {
      LOG.error("Exception occured while submitting schedule query.", e);
    }

  }

  /**
   * Updates each RunInfo for a schedule in DB.
   * 
   * @param scheduleid
   * @param queryHandle
   * @param sessionHandle
   * @param status
   * @param start
   * @param end
   * @param resultSetPath
   */
  private void updateRunInfo(String scheduleid, QueryHandle queryHandle,
      GrillSessionHandle sessionHandle, Status status, Date start, Date end,
      String resultSetPath) {
    GrillScheduleRunInfo scheduleRunInfoDAO = new GrillScheduleRunInfo();
    scheduleRunInfoDAO.setScheduleId(scheduleid);
    scheduleRunInfoDAO.setSessionHandle(sessionHandle.toString());
    scheduleRunInfoDAO.setRunHandle(queryHandle.toString());
    scheduleRunInfoDAO.setStatus(status.toString());
    scheduleRunInfoDAO.setStartTime(start.getTime());
    scheduleRunInfoDAO.setEndTime(end.getTime());
    scheduleRunInfoDAO.setResultPath(resultSetPath);
    grillSchedulerDAO.init(conf);

    try {
      grillSchedulerDAO.insertScheduleRunInfo(scheduleRunInfoDAO);
    } catch (Exception e) {
      LOG.error("Error occurred while inserting schedule run info.");
    }
  }

  @Override
  public String getOutputURI() throws GrillException {
    return executionService.getQuery(sessionHandle, queryHandle)
        .getResultSetPath();
  }

  @Override
  public String getStatus() throws GrillException {
    return executionService.getQuery(sessionHandle, queryHandle).getStatus()
        .toString();
  }
}
