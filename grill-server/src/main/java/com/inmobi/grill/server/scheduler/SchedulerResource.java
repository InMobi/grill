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

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.scheduler.QuerySchedulerService;

/**
 * queryapi resource
 * 
 * Grill Scheduler APIs.
 * 
 */
@Path("/scheduler")
public class SchedulerResource {
  public static final Logger LOG = LogManager
      .getLogger(SchedulerResource.class);
  private QuerySchedulerService querySchedulerService;

  private void checkSessionId(GrillSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
  }

  private void checkQuery(String query) {
    if (StringUtils.isBlank(query)) {
      throw new BadRequestException("Invalid query");
    }
  }

  /**
   * API to know if Scheduler service is up and running
   * 
   * @return Simple text saying it up
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Scheduler service is up";
  }

  public SchedulerResource() throws GrillException {
    querySchedulerService =
        (QuerySchedulerService) GrillServices.get().getService("scheduler");
  }

  QuerySchedulerService getSchedulerService() {
    return querySchedulerService;
  }

  /**
   * Get all the scheduled queries in the scheduler server; can be filtered with
   * state and user.
   * 
   * @param sessionid
   *          The sessionid in which user is working
   * @param state
   *          If any state is passed, all the queries in that state will be
   *          returned, otherwise all queries will be returned. Possible states
   *          are {@value QueryStatus.Status#values()}
   * @param user
   *          If any user is passed, all the queries submitted by the user will
   *          be returned, otherwise all the queries will be returned
   * @param scheduleid
   *          If any scheduleid is passed, only that particular scheduleTask
   *          will be returned.
   * 
   * @return List of {@link SchedulerQueryHandle} objects
   */
  @GET
  @Path("scheduletasks")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public List<QueryHandle> getAllScheduleTask(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user,
      @DefaultValue("") @QueryParam("scheduleid") String scheduleid) {
    checkSessionId(sessionid);
    try {
      return querySchedulerService.getAllQueries(sessionid, state, user,
          scheduleid);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Schedule a task
   * 
   * @param sessionid
   *          The session in which user is submitting the query. Any
   *          configuration set in the session will be picked up.
   * @param scheduleTask
   *          The task to run
   * @param conf
   *          The configuration for the task
   * @param jar
   * @param schedulerType
   * @param frequencyParam
   * 
   * @return SubmitStatus.
   */
  @POST
  @Path("scheduletasks")
  @Consumes({ MediaType.MULTIPART_FORM_DATA })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public String scheduleTask(
      @FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("scheduletask") Object task,
      @FormDataParam("conf") GrillConf conf,
      @FormDataParam("jarpath") String jarpath,
      @DefaultValue("DAILY") @FormDataParam("schedulertype") String schedulertype,
      @DefaultValue("") @FormDataParam("frequencyparam") String frequencyparam) {
    if (task instanceof String) {
      checkQuery((String) task);
    }
    checkSessionId(sessionid);
    try {
      return querySchedulerService.scheduleTask(task, conf, jarpath,
          schedulertype, frequencyparam);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Edit an existing schedule Task
   * 
   * @param sessionid
   * @param scheduleid
   * @param newTask
   * @param conf
   *          The configuration for the task
   * @param jar
   * @param schedulerType
   * @param frequencyParam
   * 
   * @return boolean depending on update successful
   */
  @POST
  @Path("updateschedule")
  @Consumes({ MediaType.MULTIPART_FORM_DATA })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public boolean updateTask(
      @FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("scheduleid") String scheduleid,
      @DefaultValue("") @FormDataParam("newtask") String newtask,
      @DefaultValue("") @FormDataParam("conf") GrillConf conf,
      @DefaultValue("") @FormDataParam("jarpath") String jarpath,
      @DefaultValue("") @FormDataParam("schedulertype") String schedulertype,
      @DefaultValue("") @FormDataParam("frequencyparam") String frequencyparam) {
    if (newtask instanceof String) {
      checkQuery(newtask);
    }
    checkSessionId(sessionid);
    try {
      return querySchedulerService.updateTask(newtask, conf, jarpath,
          schedulertype, frequencyparam);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Delete a schedule Task
   * 
   * @param sessionid
   * @param scheduleid
   * 
   * @return boolean depending on deletion successful
   */
  @DELETE
  @Path("deleteschedule/{scheduleid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public boolean deleteSchedule(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("scheduleid") String scheduleid) {
    checkSessionId(sessionid);
    try {
      return querySchedulerService.delete(sessionid, scheduleid);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Pause/Activate a schedule
   * 
   * @param sessionid
   * @param scheduleid
   * @param status
   * 
   * @return boolean
   */
  @POST
  @Path("statusupdate/{scheduleid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public boolean statusUpdate(@PathParam("scheduleid") String scheduleid,
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @QueryParam("status") String status) {
    checkSessionId(sessionid);
    try {
      return querySchedulerService.statusUpdate(sessionid, scheduleid, status);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

}
