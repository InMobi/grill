package com.inmobi.grill.server.scheduler;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.UUID;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.service.cli.CLIService;

import com.google.gson.Gson;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.schedule.ScheduleInfo;
import com.inmobi.grill.api.schedule.ScheduleRunInfo;
import com.inmobi.grill.api.schedule.ScheduleStatus;
import com.inmobi.grill.api.schedule.ScheduleStatus.Status;
import com.inmobi.grill.api.schedule.XExecution;
import com.inmobi.grill.api.schedule.XSchedule;
import com.inmobi.grill.api.schedule.XStartSpec;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.api.scheduler.SchedulerService;

public class SchedulerServiceImpl extends GrillService implements
    SchedulerService {

  private static final Log LOG = LogFactory.getLog(SchedulerServiceImpl.class);
  private static final String STATUS = "status";
  private final String SCHEDULE_ID = "schedule_id";
  private final String EXECUTION = "execution";
  private final String START_SPEC = "start_spec";
  private final String RESOURCE_PATH = "resource_path";
  private final String SCHEDULE_CONF = "schedule_conf";
  private final String START_TIME = "start_time";
  private final String END_TIME = "end_time";
  private final String RUN_ID = "run_id";

  public SchedulerServiceImpl(CLIService cliService) {
    super("scheduler", cliService);
  }

  @Override
  public List<String> getAllSchedules(GrillSessionHandle sessionHandle,
      String scheduleid, String status, String user) throws GrillException {
    Connection connection = SchedulerConnectionPool.getDataSource();
    List<String> scheduleIds = new ArrayList<String>();
    try {
      PreparedStatement stmt;
      final String queryString =
          "select schedule_id from schedule_info where schedule_id ilike %?% AND status ilike %?% AND username ilike %?%";
      stmt = connection.prepareStatement(queryString);
      if (scheduleid != null && !scheduleid.isEmpty()) {
        stmt.setString(1, scheduleid);
      } else {
        stmt.setString(1, "");
      }
      if (status != null && !status.isEmpty()) {
        stmt.setString(2, status);
      } else {
        stmt.setString(2, "");
      }
      if (user != null && !user.isEmpty()) {
        stmt.setString(3, user);
      } else {
        stmt.setString(3, "");
      }
      final ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        scheduleIds.add(rs.getString(SCHEDULE_ID));
      }
      stmt.close();
    } catch (final SQLException e) {
      LOG.error("Failed to submit Query", e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (final SQLException e) {
        LOG.error("Error closing connection", e);
      }
    }
    return scheduleIds;
  }

  @SuppressWarnings("unchecked")
  @Override
  public XSchedule getScheduleDefn(GrillSessionHandle sessionHandle,
      String scheduleid) throws GrillException {
    XSchedule schedule = new XSchedule();
    Connection connection = SchedulerConnectionPool.getDataSource();
    Gson gson = new Gson();

    try {
      PreparedStatement stmt;
      final String queryString =
          "select execution, start_spec, resource_path, schedule_conf, start_time, end_time from schedule_info where schedule_id = ?";
      stmt = connection.prepareStatement(queryString);
      stmt.setString(1, scheduleid);
      final ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        schedule.setExecution(gson.fromJson(rs.getClob(EXECUTION)
            .getCharacterStream(), XExecution.class));
        schedule.setStartSpec(gson.fromJson(rs.getClob(START_SPEC)
            .getCharacterStream(), XStartSpec.class));
        schedule.getResourcePath().addAll(
            gson.fromJson(rs.getClob(RESOURCE_PATH).getCharacterStream(),
                List.class));
        schedule.getScheduleConf().addAll(
            gson.fromJson(rs.getClob(SCHEDULE_CONF).getCharacterStream(),
                List.class));
        GregorianCalendar xc = new GregorianCalendar();
        xc.setTimeInMillis(rs.getLong(START_TIME));
        schedule.setStartTime(DatatypeFactory.newInstance()
            .newXMLGregorianCalendar(xc));
        xc.setTimeInMillis(rs.getLong(END_TIME));
        schedule.setEndTime(DatatypeFactory.newInstance()
            .newXMLGregorianCalendar(xc));
      }
      stmt.close();
    } catch (final SQLException e) {
      LOG.error("Failed to submit Query", e);
    } catch (DatatypeConfigurationException e) {
      LOG.error("Could not get start/end date", e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (final SQLException e) {
        LOG.error("Error closing connection", e);
      }
    }
    return schedule;
  }

  @Override
  public ScheduleInfo getGrillSchedule(GrillSessionHandle sessionHandle,
      String scheduleid) throws GrillException {
    // Return defn + status + created_on + last run result
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean delete(GrillSessionHandle sessionid, String scheduleid)
      throws GrillException {
    boolean status = true;
    Connection connection = SchedulerConnectionPool.getDataSource();
    try {
      PreparedStatement stmt;
      final String queryString =
          "DELETE from schedule_info where schedule_id = ?";
      stmt = connection.prepareStatement(queryString);
      stmt.setString(1, scheduleid);
      status = stmt.execute();
      stmt.close();
    } catch (final SQLException e) {
      LOG.error("Failed to submit Query", e);
      status = false;
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (final SQLException e) {
        LOG.error("Error closing connection", e);
      }
    }
    return status;
  }

  @Override
  public List<String> getScheduleRuns(GrillSessionHandle sessionid,
      String scheduleid) throws GrillException {
    // List down all the run_ids from schedule_run_info for a schedule
    Connection connection = SchedulerConnectionPool.getDataSource();
    List<String> runIds = new ArrayList<String>();
    try {
      PreparedStatement stmt;
      final String queryString =
          "select run_id from schedule_run_info where schedule_id = ?";
      stmt = connection.prepareStatement(queryString);
      stmt.setString(1, scheduleid);
      final ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        runIds.add(rs.getString(RUN_ID));
      }
      stmt.close();
    } catch (final SQLException e) {
      LOG.error("Failed to submit Query", e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (final SQLException e) {
        LOG.error("Error closing connection", e);
      }
    }
    return runIds;
  }

  @Override
  public ScheduleRunInfo getScheduleRunDetail(GrillSessionHandle sessionid,
      String scheduleid, String runHandle) throws GrillException {
    // Will return the QueryDetails using get call on query_run_details
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean scheduleTask(GrillSessionHandle sessionid, XSchedule schedule)
      throws GrillException {
    boolean status = true;
    Connection connection = SchedulerConnectionPool.getDataSource();
    Gson gson = new Gson();

    String userName = getSession(sessionid).getUserName();
    String execJson = gson.toJson(schedule.getExecution());
    try {
      Clob execClob = connection.createClob();
      execClob.setString(0, execJson);

      String startSpecJson = gson.toJson(schedule.getStartSpec());
      Clob startSpecClob = connection.createClob();
      startSpecClob.setString(0, startSpecJson);

      String resPathJson = gson.toJson(schedule.getResourcePath());
      Clob resPathClob = connection.createClob();
      resPathClob.setString(0, resPathJson);

      String scheConJson = gson.toJson(schedule.getScheduleConf());
      Clob scheConClob = connection.createClob();
      scheConClob.setString(0, scheConJson);

      PreparedStatement stmt;
      final String queryString =
          "INSERT into schedule_info(schedule_id, execution, start_spec, resource_path, schedule_conf, start_time, end_time, username, status, created_on) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'NEW', now())";
      stmt = connection.prepareStatement(queryString);
      stmt.setString(1, UUID.randomUUID().toString());
      stmt.setClob(2, execClob);
      stmt.setClob(3, startSpecClob);
      stmt.setClob(4, resPathClob);
      stmt.setClob(5, scheConClob);
      stmt.setInt(6, schedule.getStartTime().getMillisecond());
      stmt.setInt(7, schedule.getEndTime().getMillisecond());
      stmt.setString(8, userName);
      stmt.executeUpdate();
      stmt.close();
    } catch (final SQLException e) {
      LOG.error("Failed to submit Query", e);
      status = false;
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (final SQLException e) {
        LOG.error("Error closing connection", e);
      }
    }
    return status;
  }

  @Override
  public boolean updateSchedule(GrillSessionHandle sessionid,
      String scheduleid, XSchedule newSchedule) throws GrillException {
    boolean status = true;
    Connection connection = SchedulerConnectionPool.getDataSource();
    Gson gson = new Gson();

    String execJson = gson.toJson(newSchedule.getExecution());
    try {
      Clob execClob = connection.createClob();
      execClob.setString(0, execJson);

      String startSpecJson = gson.toJson(newSchedule.getStartSpec());
      Clob startSpecClob = connection.createClob();
      startSpecClob.setString(0, startSpecJson);

      String resPathJson = gson.toJson(newSchedule.getResourcePath());
      Clob resPathClob = connection.createClob();
      resPathClob.setString(0, resPathJson);

      String scheConJson = gson.toJson(newSchedule.getScheduleConf());
      Clob scheConClob = connection.createClob();
      scheConClob.setString(0, scheConJson);

      PreparedStatement stmt;
      final String queryString =
          "update schedule_info set execution=?, start_spec=?, resource_path=?, schedule_conf=?, start_time=?, end_time=?, modified_on = now() where schedule_id = ?";
      stmt = connection.prepareStatement(queryString);
      stmt.setClob(1, execClob);
      stmt.setClob(2, startSpecClob);
      stmt.setClob(3, resPathClob);
      stmt.setClob(4, scheConClob);
      stmt.setInt(5, newSchedule.getStartTime().getMillisecond());
      stmt.setInt(6, newSchedule.getEndTime().getMillisecond());
      stmt.setString(7, scheduleid);
      stmt.executeUpdate();
      stmt.close();
    } catch (final SQLException e) {
      LOG.error("Failed to submit Query", e);
      status = false;
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (final SQLException e) {
        LOG.error("Error closing connection", e);
      }
    }
    return status;
  }

  @Override
  public boolean updateStatus(GrillSessionHandle sessionid, String scheduleid,
      Status newstatus) throws GrillException {
    // Update status of scheduleId to the new status, whenever there is
    // newStatus = 'SCHEDULED', schedule the scheduleid, in case its set to
    // 'PAUSED', remove from schedule.
    // Whenever there is a status change, a schedule update will be
    // needed
    boolean status = true;
    Connection connection = SchedulerConnectionPool.getDataSource();
    PreparedStatement stmt;

    try {
      final String getStatusQuery =
          "select status from schedule_info where schedule_id = ?";
      stmt = connection.prepareStatement(getStatusQuery);
      stmt.setString(1, scheduleid);
      Status oldStatus = null;
      final ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        oldStatus = Status.valueOf(rs.getString(STATUS));
      }
      if (!ScheduleStatus.isValidTransition(oldStatus, newstatus)) {
        throw new GrillException("Not a valid state transfer from : "
            + oldStatus.name() + " to : " + newstatus.name());
      }

      final String setStatusQuery =
          "update schedule_info set status = ? where schedule_id = ?";
      stmt = connection.prepareStatement(setStatusQuery);
      stmt.setString(1, newstatus.toString());
      stmt.setString(2, scheduleid);
      status = stmt.execute();
      stmt.close();
    } catch (final SQLException e) {
      LOG.error("Failed to submit Query", e);
      status = false;
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (final SQLException e) {
        LOG.error("Error closing connection", e);
      }
    }
    XSchedule schedule = getScheduleDefn(sessionid, scheduleid);
    new ScheduleJob(schedule, newstatus, scheduleid);
    return status;
  }

  @Override
  public boolean rerun(GrillSessionHandle sessionHandle, String scheduleid,
      String runid) throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }
}
