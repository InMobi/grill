package com.inmobi.grill.server.api.scheduler;

import java.util.List;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.schedule.ScheduleInfo;
import com.inmobi.grill.api.schedule.ScheduleRunInfo;
import com.inmobi.grill.api.schedule.ScheduleStatus;
import com.inmobi.grill.api.schedule.ScheduleStatus.Status;
import com.inmobi.grill.api.schedule.XSchedule;

/*
 * #%L
 * Grill API for server and extensions
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

public interface SchedulerService {
  public static final String NAME = "scheduler";

  /**
   * 
   * @param sessionid
   * @param state
   * @param user
   * @param scheduleid
   * @param type
   * @return List of ScheduleHandles
   * @throws GrillException
   */
  public List<String> getAllSchedules(GrillSessionHandle sessionHandle,
      String scheduleid, String status, String user) throws GrillException;

  /**
   * 
   * @param sessionid
   * @param scheduleid
   * @return QueryHandle
   * @throws GrillException
   */
  public XSchedule getScheduleDefn(GrillSessionHandle sessionHandle,
      String scheduleid) throws GrillException;

  /**
   * 
   * @param sessionHandle
   * @param scheduleid
   * @return ScheduleInfo object which has detailed info about schedule
   * @throws GrillException
   */
  public ScheduleInfo getGrillSchedule(GrillSessionHandle sessionHandle,
      String scheduleid) throws GrillException;

  /**
   * 
   * @param sessionid
   * @param scheduleid
   * @return
   * @throws GrillException
   */
  public boolean delete(GrillSessionHandle sessionid, String scheduleid)
      throws GrillException;

  /**
   * 
   * @param sessionid
   * @param scheduleid
   * @return List of RunHandles for a schedule
   * @throws GrillException
   */
  public List<String> getScheduleRuns(GrillSessionHandle sessionid,
      String scheduleid) throws GrillException;

  /**
   * 
   * @param sessionid
   * @param scheduleid
   * @param runHandle
   * @return Info about a schedule
   * @throws GrillException
   */
  public ScheduleRunInfo getScheduleRunDetail(GrillSessionHandle sessionid,
      String scheduleid, String runHandle) throws GrillException;

  /**
   * 
   * @param sessionid
   * @param grillSchedule
   * @return true/false, based of schedule success or not
   * @throws GrillException
   */
  public boolean scheduleTask(GrillSessionHandle sessionid, XSchedule schedule)
      throws GrillException;

  /**
   * 
   * @param sessionid
   * @param scheduleid
   * @param newGrillSchedule
   * @return
   * @throws GrillException
   */
  public boolean updateSchedule(GrillSessionHandle sessionid,
      String scheduleid, XSchedule newSchedule) throws GrillException;

  /**
   * 
   * @param sessionid
   * @param scheduleid
   * @param newstatus
   * @return
   * @throws GrillException
   */
  public boolean updateStatus(GrillSessionHandle sessionid, String scheduleid,
      Status newstatus) throws GrillException;

  /**
   * 
   * @param sessionHandle
   * @param scheduleid
   * @param runid
   * @return
   * @throws GrillException
   */
  public boolean rerun(GrillSessionHandle sessionHandle, String scheduleid,
      String runid) throws GrillException;

}
