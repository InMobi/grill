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
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.api.scheduler.QuerySchedulerService;

public class QuerySchedulerServiceImpl extends GrillService implements QuerySchedulerService {

  public QuerySchedulerServiceImpl(CLIService cliService) {
    super("scheduler", cliService);
  }

  @Override
  public String scheduleTask(Object task, GrillConf conf, String jarPath,
      String schedulerType, String frequencyParams) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<QueryHandle> getAllQueries(GrillSessionHandle sessionid,
      String state, String user, String scheduleid) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean updateTask(Object newtask, GrillConf conf, String jarpath,
      String schedulertype, String frequencyparam) throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean delete(GrillSessionHandle sessionid, String scheduleid)
      throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean statusUpdate(GrillSessionHandle sessionid, String scheduleid,
      String status) throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }
}
