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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.CLIService;
import org.quartz.SchedulerException;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.schedule.ScheduleInfo;
import com.inmobi.grill.api.schedule.ScheduleRunInfo;
import com.inmobi.grill.api.schedule.ScheduleStatus;
import com.inmobi.grill.api.schedule.ScheduleStatus.Status;
import com.inmobi.grill.api.schedule.XSchedule;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.api.scheduler.SchedulerService;

public class SchedulerServiceImpl extends GrillService implements
    SchedulerService {

  private GrillSchedulerDAO grillSchedulerDao;
  private Configuration conf;

  public SchedulerServiceImpl(CLIService cliService) {
    super("scheduler", cliService);
  }

  @Override
  public List<String> getAllSchedules(GrillSessionHandle sessionHandle,
      String status, String user) throws GrillException {
    conf = new Configuration();
    grillSchedulerDao.init(conf);
    try {
      if ((status != null && !status.isEmpty())
          && (user != null && !user.isEmpty())) {
        return grillSchedulerDao.getScheduleIds(status, user);
      } else if (status != null && !status.isEmpty()) {
        return grillSchedulerDao.getScheduleIds(ScheduleStatus.Status
            .valueOf(status));
      } else if (user != null && !user.isEmpty()) {
        return grillSchedulerDao.getScheduleIds(user);
      } else {
        return grillSchedulerDao.getScheduleIds();
      }
    } catch (Exception e) {
      throw new GrillException("Error getting all schedule_id", e);
    }
  }

  @Override
  public XSchedule getScheduleDefn(GrillSessionHandle sessionHandle,
      String scheduleid) throws GrillException {
    conf = new Configuration();
    grillSchedulerDao.init(conf);
    try {
      return grillSchedulerDao.getScheduleDefn(scheduleid);
    } catch (Exception e) {
      throw new GrillException("Error getting ScheduleDefn", e);
    }
  }

  @Override
  public ScheduleInfo getGrillSchedule(GrillSessionHandle sessionHandle,
      String scheduleid) throws GrillException {
    // Return defn + status + created_on + last run result
    grillSchedulerDao.init(conf);
    return grillSchedulerDao.getScheduleDetails(scheduleid);
  }

  @Override
  public boolean delete(GrillSessionHandle sessionid, String scheduleid)
      throws GrillException {
    boolean status = true;
    ScheduleJob job = new ScheduleJob();
    try {
      job.deschedule(scheduleid);
    } catch (SchedulerException e) {
      status = false;
      new GrillException("Unable to delete the schedule from quartz.", e);
    }
    conf = new Configuration();
    grillSchedulerDao.init(conf);
    grillSchedulerDao.deleteSchedule(scheduleid);
    return status;
  }

  @Override
  public List<String> getScheduleRuns(GrillSessionHandle sessionid,
      String scheduleid) throws GrillException {
    // List down all the run_ids from schedule_run_info for a schedule
    conf = new Configuration();
    grillSchedulerDao.init(conf);
    return grillSchedulerDao.getScheduleRunIds(scheduleid);
  }

  @Override
  public ScheduleRunInfo getScheduleRunDetail(GrillSessionHandle sessionid,
      String scheduleid, String runHandle) throws GrillException {
    // Will return the QueryDetails using get call on query_run_details
    conf = new Configuration();
    grillSchedulerDao.init(conf);
    return grillSchedulerDao.getRunInfo(scheduleid, runHandle);
  }

  @Override
  public String scheduleTask(GrillSessionHandle sessionid, XSchedule schedule)
      throws GrillException {
    conf = new Configuration();
    grillSchedulerDao.init(conf);
    String username = getSession(sessionid).getUserName();
    return grillSchedulerDao.insertScheduleInfo(schedule, username);
  }

  @Override
  public boolean updateSchedule(GrillSessionHandle sessionid,
      String scheduleid, XSchedule newSchedule) throws GrillException {
    boolean status = true;
    conf = new Configuration();
    grillSchedulerDao.init(conf);
    grillSchedulerDao.updateSchedule(scheduleid, newSchedule);
    return status;
  }

  @Override
  public void updateStatus(GrillSessionHandle sessionid, String scheduleid,
      String newstatus) throws GrillException {
    // Update status of scheduleId to the new status, whenever there is
    // newStatus = 'SCHEDULED', schedule the scheduleid, in case its set to
    // 'PAUSED', remove from schedule.
    // Whenever there is a status change, a schedule update will be
    // needed
    conf = new Configuration();
    grillSchedulerDao.init(conf);
    grillSchedulerDao.updateStatus(scheduleid, Status.valueOf(newstatus));
    XSchedule schedule = getScheduleDefn(sessionid, scheduleid);
    new ScheduleJob(schedule, Status.valueOf(newstatus), scheduleid);
  }

  @Override
  public boolean rerun(GrillSessionHandle sessionHandle, String scheduleid,
      String runid) throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }
}
