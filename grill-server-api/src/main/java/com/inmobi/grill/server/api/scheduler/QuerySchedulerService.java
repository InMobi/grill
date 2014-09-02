package com.inmobi.grill.server.api.scheduler;

import java.util.List;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.QueryHandle;

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

public interface QuerySchedulerService {
  public static final String NAME = "scheduler";

  /**
   * Submit a query for scheduling
   * 
   * @param task
   *          (query/etc)
   * @param conf
   * @param jarPath
   *          : jars to be used for this query
   * @param schedulerType
   *          : type of scheduleQuery
   * @param frequencyParams
   *          : in case the scheduletype is custom read frequency params to
   *          schedule.
   * 
   * @return ScheduleId if schedule is successful
   */
  public String scheduleTask(Object task, GrillConf conf, String jarPath,
      String schedulerType, String frequencyParams) throws GrillException;

  /**
   * 
   * @param sessionid
   * @param state
   * @param user
   * @param scheduleid
   * @return
   * @throws GrillException
   */
  public List<QueryHandle> getAllQueries(GrillSessionHandle sessionid,
      String state, String user, String scheduleid) throws GrillException;

  /**
   * 
   * @param newtask
   * @param conf
   * @param jarpath
   * @param schedulertype
   * @param frequencyparam
   * @return
   * @throws GrillException
   */
  public boolean updateTask(Object newtask, GrillConf conf, String jarpath,
      String schedulertype, String frequencyparam) throws GrillException;

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
   * @param status
   * @return
   * @throws GrillException
   */
  public boolean statusUpdate(GrillSessionHandle sessionid, String scheduleid,
      String status) throws GrillException;

}
