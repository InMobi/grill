package com.inmobi.grill.server.scheduler;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.UUID;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.hive.com.esotericsoftware.minlog.Log;

import com.google.gson.Gson;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.schedule.ScheduleInfo;
import com.inmobi.grill.api.schedule.ScheduleRunInfo;
import com.inmobi.grill.api.schedule.ScheduleStatus;
import com.inmobi.grill.api.schedule.ScheduleStatus.Status;
import com.inmobi.grill.api.schedule.XExecution;
import com.inmobi.grill.api.schedule.XSchedule;
import com.inmobi.grill.api.schedule.XStartSpec;
import com.inmobi.grill.server.GrillServerDAO;
import com.inmobi.grill.server.api.scheduler.GrillScheduleInfo;
import com.inmobi.grill.server.api.scheduler.GrillScheduleRunInfo;

public class GrillSchedulerDAO extends GrillServerDAO {
  private Gson gson = new Gson();

  public GrillSchedulerDAO() {
    super();
  }

  /**
   * Method to create ScheduleInfo table, this is required for embedded grill
   * server. For production server we will not be creating tables as it would be
   * created upfront.
   */
  public void createScheduleInfoTable() throws GrillException {
    String sql =
        "CREATE TABLE if not exists schedule_info (schedule_id varchar(255) not null unique,"
            + "execution clob not null, startspec clob not null,"
            + "resourcepath clob, schedule_conf clob, starttime bigint,"
            + "endtime bigint, username varchar(255), status varchar(255), "
            + "createdon bigint)";
    try {
      createTable(sql);
      ds.getConnection().commit();
    } catch (SQLException e) {
      throw new GrillException("Unable to create ScheduleInfo table.", e);
    }
  }

  public void dropScheduleInfoTable() throws Exception {
    try {
      dropTable("drop table schedule_info");
    } catch (SQLException e) {
      throw new GrillException("Unable to drop schedule_info table", e);
    }
  }

  /**
   * Method to create ScheduleRunInfo table, this is required for embedded grill
   * server. For production server we will not be creating tables as it would be
   * created upfront.
   */
  public void createScheduleRunInfoTable() throws GrillException {
    String sql =
        "CREATE TABLE if not exists schedule_run_info (schedule_id varchar(255) not null unique,"
            + "sessionhandle varchar(255) not null, runhandle varchar(255) not null,"
            + "starttime bigint, endtime bigint, resultpath varchar(255),"
            + "status varchar(255), query varchar(255))";
    try {
      createTable(sql);
      ds.getConnection().commit();
    } catch (SQLException e) {
      throw new GrillException("Unable to create ScheduleRunInfo table.", e);
    }
  }

  public void dropScheduleRunInfoTable() throws Exception {
    try {
      dropTable("drop table schedule_run_info");
    } catch (SQLException e) {
      throw new GrillException("Unable to drop schedule_run_info table", e);
    }
  }

  /**
   * DAO method to insert a new Schedule info into Table
   * 
   * @param scheduleInfoDAO
   *          to be inserted
   */
  public String insertScheduleInfo(XSchedule schedule, String username)
      throws GrillException {
    String scheduleId = UUID.randomUUID().toString();
    GrillScheduleInfo scheduleInfoDAO = new GrillScheduleInfo();
    scheduleInfoDAO.setScheduleId(scheduleId);
    scheduleInfoDAO.setUsername(username);
    String execJson = gson.toJson(schedule.getExecution());
    String startSpecJson = gson.toJson(schedule.getStartSpec());
    String resPathJson = gson.toJson(schedule.getResourcePath());
    String scheConJson = gson.toJson(schedule.getScheduleConf());
    scheduleInfoDAO.setStartTime(schedule.getStartTime().getMillisecond());
    scheduleInfoDAO.setEndTime(schedule.getEndTime().getMillisecond());
    try {
      Clob execClob = ds.getConnection().createClob();
      execClob.setString(0, execJson);
      scheduleInfoDAO.setExecution(execClob);

      Clob startSpecClob = ds.getConnection().createClob();
      startSpecClob.setString(0, startSpecJson);
      scheduleInfoDAO.setStartSpec(startSpecClob);

      Clob resPathClob = ds.getConnection().createClob();
      resPathClob.setString(0, resPathJson);
      scheduleInfoDAO.setResourcePath(resPathClob);

      Clob scheConClob = ds.getConnection().createClob();
      scheConClob.setString(0, scheConJson);
      scheduleInfoDAO.setScheduleConf(scheConClob);
    } catch (SQLException e) {
      throw new GrillException("Error setting clob.", e);
    }

    String sql =
        "INSERT into schedule_info(schedule_id, execution, start_spec, resource_path, schedule_conf, start_time, end_time, username, status, created_on) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'NEW', now())";
    QueryRunner runner = new QueryRunner(ds);
    try {
      runner.update(sql, scheduleInfoDAO.getScheduleId(),
          scheduleInfoDAO.getExecution(), scheduleInfoDAO.getStartSpec(),
          scheduleInfoDAO.getResourcePath(), scheduleInfoDAO.getScheduleConf(),
          scheduleInfoDAO.getStartTime(), scheduleInfoDAO.getEndTime(),
          scheduleInfoDAO.getUsername(), scheduleInfoDAO.getStatus(),
          scheduleInfoDAO.getCreated_on());
    } catch (SQLException e) {
      throw new GrillException("Error inserting schedule task in DB", e);
    }
    return scheduleId;
  }

  /**
   * DAO method to insert a new Schedule Run data into Table
   * 
   * @param runInfoDAO
   *          to be inserted
   */
  public void insertScheduleRunInfo(GrillScheduleRunInfo runInfoDAO)
      throws Exception {
    String sql =
        "INSERT into schedule_run_info(schedule_id, session_handle, run_handle, start_time, end_time, status, result_path) VALUES(?, ?, ?, ?, ?, ?, ?)";
    QueryRunner runner = new QueryRunner(ds);
    try {
      runner.update(sql, runInfoDAO.getScheduleId(),
          runInfoDAO.getSessionHandle(), runInfoDAO.getRunHandle(),
          runInfoDAO.getStartTime(), runInfoDAO.getEndTime(),
          runInfoDAO.getStatus(), runInfoDAO.getResultPath());
    } catch (SQLException e) {
      throw new Exception(e);
    }
  }

  /**
   * DAO method to get all scheduleIds with(out) filters from schedule_info
   * table.
   * 
   * @param query
   *          to get the scheduleIds.
   */
  public List<String> getScheduleIds(String status, String user)
      throws Exception {
    ResultSetHandler<List<String>> rsh =
        new BeanListHandler<String>(String.class);
    String sql =
        "select schedule_id from scheduler_info where status=? and username=?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      return runner.query(sql, rsh, status, user);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public List<String> getScheduleIds(String user) throws Exception {
    ResultSetHandler<List<String>> rsh =
        new BeanListHandler<String>(String.class);
    String sql = "select schedule_id from scheduler_info where username=?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      return runner.query(sql, rsh, user);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public List<String> getScheduleIds(ScheduleStatus.Status status)
      throws Exception {
    ResultSetHandler<List<String>> rsh =
        new BeanListHandler<String>(String.class);
    String sql = "select schedule_id from scheduler_info where status=?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      return runner.query(sql, rsh, status.toString());
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public List<String> getScheduleIds() throws Exception {
    ResultSetHandler<List<String>> rsh =
        new BeanListHandler<String>(String.class);
    String sql = "select schedule_id from scheduler_info";
    QueryRunner runner = new QueryRunner(ds);
    try {
      return runner.query(sql, rsh);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public XSchedule getScheduleDefn(String schedule_id) throws GrillException {
    ResultSetHandler<GrillScheduleInfo> rsh =
        new BeanHandler<GrillScheduleInfo>(GrillScheduleInfo.class);
    String sql = "select * from schedule_info where schedule_id = ?";
    QueryRunner runner = new QueryRunner(ds);
    GrillScheduleInfo scheduleInfoDAO = null;
    XSchedule schedule = new XSchedule();
    try {
      scheduleInfoDAO = runner.query(sql, rsh, schedule_id);
      schedule.setExecution(gson.fromJson(scheduleInfoDAO.getExecution()
          .getCharacterStream(), XExecution.class));
      schedule.setStartSpec(gson.fromJson(scheduleInfoDAO.getStartSpec()
          .getCharacterStream(), XStartSpec.class));
      schedule.getResourcePath().addAll(
          gson.fromJson(scheduleInfoDAO.getResourcePath().getCharacterStream(),
              List.class));
      schedule.getScheduleConf().addAll(
          gson.fromJson(scheduleInfoDAO.getScheduleConf().getCharacterStream(),
              List.class));
      GregorianCalendar xc = new GregorianCalendar();
      xc.setTimeInMillis(scheduleInfoDAO.getStartTime());
      schedule.setStartTime(DatatypeFactory.newInstance()
          .newXMLGregorianCalendar(xc));
      xc.setTimeInMillis(scheduleInfoDAO.getEndTime());
      schedule.setEndTime(DatatypeFactory.newInstance()
          .newXMLGregorianCalendar(xc));
    } catch (SQLException e) {
      throw new GrillException("Error getting schedule defn.", e);
    } catch (DatatypeConfigurationException e1) {
      throw new GrillException("Error getting schedule defn.", e1);
    }
    return schedule;
  }

  public boolean updateStatus(String scheduleid, Status newStatus)
      throws GrillException {
    boolean status = true;
    ResultSetHandler<String> rsh = new BeanHandler<String>(String.class);
    final String getStatusQuery =
        "select status from schedule_info where schedule_id = ?";
    QueryRunner runner = new QueryRunner(ds);
    Status oldStatus = null;
    try {
      oldStatus = Status.valueOf(runner.query(getStatusQuery, rsh));
    } catch (Exception e) {
      status = false;
      throw new GrillException(
          "Error getting old state to validate state transfer.", e);
    }
    if (!ScheduleStatus.isValidTransition(oldStatus, newStatus)) {
      status = false;
      throw new GrillException("Not a valid state transfer from : "
          + oldStatus.name() + " to : " + newStatus.name());
    }

    String sql = "update schedule_info set status = ? where schedule_id = ?";
    try {
      runner.update(sql, newStatus, scheduleid);
    } catch (SQLException e) {
      status = false;
      throw new GrillException("Error in changing status of schedule.", e);
    }
    return status;
  }

  public void updateSchedule(String scheduleid, XSchedule newSchedule)
      throws GrillException {
    String execJson = gson.toJson(newSchedule.getExecution());
    try {
      Clob execClob = ds.getConnection().createClob();
      execClob.setString(0, execJson);

      String startSpecJson = gson.toJson(newSchedule.getStartSpec());
      Clob startSpecClob = ds.getConnection().createClob();
      startSpecClob.setString(0, startSpecJson);

      String resPathJson = gson.toJson(newSchedule.getResourcePath());
      Clob resPathClob = ds.getConnection().createClob();
      resPathClob.setString(0, resPathJson);

      String scheConJson = gson.toJson(newSchedule.getScheduleConf());
      Clob scheConClob = ds.getConnection().createClob();
      scheConClob.setString(0, scheConJson);

      final String queryString =
          "update schedule_info set execution=?, start_spec=?, resource_path=?, schedule_conf=?, start_time=?, end_time=?, modified_on = now() where schedule_id = ?";

      QueryRunner runner = new QueryRunner(ds);
      runner.update(queryString, execClob, startSpecClob, resPathClob,
          scheConClob, newSchedule.getStartTime().getMillisecond(), newSchedule
              .getEndTime().getMillisecond(), scheduleid);
    } catch (SQLException e) {
      throw new GrillException("Error while updating the schedule.", e);
    }
  }

  public List<String> getScheduleRunIds(String scheduleId) {
    ResultSetHandler<List<String>> rsh =
        new BeanListHandler<String>(String.class);
    String sql =
        "select run_handle from schedule_run_info where schedule_id = ?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      return runner.query(sql, rsh, scheduleId);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void deleteSchedule(String scheduleId) {
    String deleteScheduleInfo = "DELETE from schedule_info where schedule_id=?";
    String deleteScheduleRunInfo =
        "DELETE from schedule_run_info where schedule_id = ?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      runner.update(deleteScheduleInfo, scheduleId);
      runner.update(deleteScheduleRunInfo, scheduleId);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public ScheduleInfo getScheduleDetails(String schedule_id)
      throws GrillException {
    ResultSetHandler<String> rsh = new ResultSetHandler<String>() {
      @Override
      public String handle(ResultSet rs) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        if (rs.next()) {
          stringBuilder.append(rs.getString(1)).append("#");
          stringBuilder.append(rs.getString(2));
        }
        return stringBuilder.toString();
      }
    };
    ScheduleInfo scheduleInfo =
        new ScheduleInfo(schedule_id, new XSchedule(), "", "", "", "");
    XSchedule schedule = getScheduleDefn(schedule_id);
    String query =
        "SELECT username, status from schedule_info where schedule_id=?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      String result = runner.query(query, rsh, schedule_id);
      scheduleInfo.setSubmittedUser(result.split("#")[0]);
      scheduleInfo.setScheduleStatus(result.split("#")[1]);
    } catch (SQLException e) {
      throw new GrillException("Error in getting scheduleInfo.", e);
    }

    String runDataQuery =
        "SELECT run_id, result_path from schedule_run_info where schedule_id=?";
    try {
      String result = runner.query(runDataQuery, rsh, schedule_id);
      scheduleInfo.setLastRunInstanceId(result.split("#")[0]);
      scheduleInfo.setLatestResultSetPath(result.split("#")[1]);
    } catch (SQLException e) {
      throw new GrillException("Error in getting scheduleInfo.", e);
    }
    scheduleInfo.setSchedule(schedule);
    return scheduleInfo;
  }

  public ScheduleRunInfo getRunInfo(String scheduleId, String runHandle)
      throws GrillException {
    ResultSetHandler<ScheduleRunInfo> rsh =
        new BeanHandler<ScheduleRunInfo>(ScheduleRunInfo.class);
    String runDataQuery =
        "SELECT schedule_id, run_handle, start_time, end_time, result_path, status"
            + " from schedule_run_info where schedule_id=? and run_handle=?";
    QueryRunner runner = new QueryRunner(ds);
    try {
      return runner.query(runDataQuery, rsh, scheduleId, runHandle);
    } catch (SQLException e) {
      Log.error("Error in getting scheduleInfo.", e);
    }
    return null;
  }
}
