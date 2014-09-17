package com.inmobi.grill.server.scheduler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.google.gson.Gson;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.query.QueryExecutionService;
import com.inmobi.grill.server.session.HiveSessionService;

public class ScheduleQueryExecution implements Execution {
  private Gson gson = new Gson();
  private static final Log LOG = LogFactory
      .getLog(ScheduleQueryExecution.class);
  private GrillEventService eventService;
  private GrillSessionHandle sessionHandle;
  private QueryHandle queryHandle;
  private static final long SLEEP_DURATION = 10 * 1000;
  private Date start;
  private Date end;

  public ScheduleQueryExecution() {

  }

  protected GrillEventService getEventService() {
    if (eventService == null) {
      eventService =
          (GrillEventService) GrillServices.get().getService(
              GrillEventService.NAME);
      if (eventService == null) {
        throw new NullPointerException("Could not get event service");
      }
    }
    return eventService;
  }

  private QueryExecutionService getQuerySvc() {
    return (QueryExecutionService) GrillServices.get().getService(
        QueryExecutionService.NAME);
  }

  private HiveSessionService getSessionSvc() {
    return (HiveSessionService) GrillServices.get().getService(
        HiveSessionService.NAME);
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
    String conf = dataMap.getString("conf");
    String scheduleid = dataMap.getString("scheduleid");
    String session_db = dataMap.getString("session_db");
    List<String> resource_path =
        gson.fromJson(dataMap.getString("resource_path"), List.class);

    GrillConf grillConf = gson.fromJson(conf, GrillConf.class);

    try {
      sessionHandle =
          getSessionSvc().openSession("", "", new HashMap<String, String>());
    } catch (GrillException e) {
      LOG.error("Exception occured while creating session for schedule query.",
          e);
    }

    for (String path : resource_path) {
      getSessionSvc().addResource(sessionHandle, "jar/file", path);
    }

    QueryStatus.Status status = Status.NEW;
    try {
      queryHandle = getQuerySvc().executeAsync(sessionHandle, query, grillConf);
      while (status != Status.SUCCESSFUL && status != Status.CANCELED
          && status != Status.CLOSED && status != Status.FAILED) {
        try {
          Thread.sleep(SLEEP_DURATION);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        status =
            getQuerySvc().getQuery(sessionHandle, queryHandle).getStatus()
                .getStatus();
      }
      end = new Date();
      updateRunStatus(scheduleid, queryHandle, sessionHandle, status, start,
          end, getQuerySvc().getQuery(sessionHandle, queryHandle)
              .getResultSetPath());
    } catch (GrillException e) {
      LOG.error("Exception occured while submitting schedule query.", e);
    }

  }

  private void updateRunStatus(String scheduleid, QueryHandle queryHandle,
      GrillSessionHandle sessionHandle, Status status, Date start, Date end,
      String resultSetPath) {
    Connection connection = SchedulerConnectionPool.getDataSource();

    try {
      PreparedStatement stmt;
      final String queryString =
          "INSERT into schedule_run_info(schedule_id, session_handle, run_handle, start_time, end_time, status, result_path) VALUES(?, ?, ?, ?, ?, ?, ?)";
      stmt = connection.prepareStatement(queryString);
      stmt.setString(1, scheduleid);
      stmt.setString(2, sessionHandle.toString());
      stmt.setString(3, queryHandle.getHandleId().toString());
      stmt.setLong(4, start.getTime());
      stmt.setLong(5, end.getTime());
      stmt.setString(6, status.toString());
      stmt.setString(7, resultSetPath);
      stmt.executeUpdate();
      stmt.close();
    } catch (final SQLException e) {
      LOG.error("Failed to update Run Info in DB", e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (final SQLException e) {
        LOG.error("Error closing connection", e);
      }
    }
  }
}
