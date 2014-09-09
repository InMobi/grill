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

import org.apache.hive.service.cli.CLIService;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.schedule.GrillScheduleHandle;
import com.inmobi.grill.api.schedule.GrillScheduleRunHandle;
import com.inmobi.grill.api.schedule.ScheduleInfo;
import com.inmobi.grill.api.schedule.ScheduleRunInfo;
import com.inmobi.grill.api.schedule.ScheduleStatus;
import com.inmobi.grill.api.schedule.XSchedule;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.api.scheduler.SchedulerService;

public class QuerySchedulerServiceImpl extends GrillService implements SchedulerService {

  public QuerySchedulerServiceImpl(CLIService cliService) {
    super("scheduler", cliService);
  }

  @Override
  public List<GrillScheduleHandle> getAllSchedules(
      GrillSessionHandle sessionHandle, GrillScheduleHandle scheduleid,
      String state, String user, String type) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public XSchedule getScheduleDefn(GrillSessionHandle sessionHandle,
      GrillScheduleHandle scheduleid) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ScheduleInfo getGrillSchedule(GrillSessionHandle sessionHandle,
      GrillScheduleHandle scheduleid) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean updateSchedule(Object newtask, GrillConf conf, String jarpath,
      String schedulertype, String frequencyparam) throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean delete(GrillSessionHandle sessionid,
      GrillScheduleHandle scheduleid) throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<GrillScheduleRunHandle> getScheduleRuns(
      GrillSessionHandle sessionid, GrillScheduleHandle scheduleid)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ScheduleRunInfo getScheduleRunDetail(GrillSessionHandle sessionid,
      GrillScheduleHandle scheduleid, GrillScheduleRunHandle runHandle)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean scheduleTask(GrillSessionHandle sessionid, XSchedule schedule)
      throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean updateSchedule(GrillSessionHandle sessionid,
      GrillScheduleHandle scheduleid, XSchedule newSchedule)
      throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean updateStatus(GrillSessionHandle sessionid,
      GrillScheduleHandle scheduleid, ScheduleStatus newstatus)
      throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean rerun(GrillSessionHandle sessionHandle,
      GrillScheduleHandle scheduleid, GrillScheduleRunHandle runid)
      throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }
 }
