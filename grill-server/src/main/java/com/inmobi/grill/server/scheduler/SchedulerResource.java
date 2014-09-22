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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.schedule.ObjectFactory;
import com.inmobi.grill.api.schedule.ScheduleInfo;
import com.inmobi.grill.api.schedule.ScheduleRunInfo;
import com.inmobi.grill.api.schedule.ScheduleStatus;
import com.inmobi.grill.api.schedule.XSchedule;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.scheduler.SchedulerService;

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
  private SchedulerService schedulerService;
  public static final ObjectFactory xScheduleObjectFactory =
      new ObjectFactory();

  private void checkSessionId(GrillSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
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
    schedulerService =
        (SchedulerService) GrillServices.get().getService("scheduler");
  }

  SchedulerService getSchedulerService() {
    return schedulerService;
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
   *          are {@value ScheduleStatus.Status#values()}
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
  @Path("/schedules")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public List<String> getAllSchedule(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("status") String status,
      @DefaultValue("") @QueryParam("user") String user) {
    checkSessionId(sessionid);
    try {
      return schedulerService.getAllSchedules(sessionid, status,
          user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get the schedule Definition for a particular scheduleid
   * 
   * @param sessionid
   * @param scheduleid
   * @return schedule
   */
  @GET
  @Path("/schedules/{scheduleid}/defn")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public JAXBElement<XSchedule> getScheduleDefn(
      @QueryParam("sessionid") GrillSessionHandle sessionHandle,
      @PathParam("scheduleid") String scheduleid) {
    checkSessionId(sessionHandle);
    try {
      return xScheduleObjectFactory.createXSchedule(schedulerService
          .getScheduleDefn(sessionHandle, scheduleid));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get the schedule info for a particular scheduleid
   * 
   * @param sessionid
   * @param scheduleid
   * @return scheduleInfo, which gives detailed view of a schedule than just
   *         defn.
   */
  @GET
  @Path("/schedules/{scheduleid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public ScheduleInfo getScheduleInfo(
      @QueryParam("sessionid") GrillSessionHandle sessionHandle,
      @PathParam("scheduleid") String scheduleid) {
    checkSessionId(sessionHandle);
    try {
      return schedulerService.getGrillSchedule(sessionHandle, scheduleid);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Gives info about all the runs of a schedule : Returns runHandles.
   * 
   * @param sessionid
   * @param scheduleid
   * @param duration
   * @return RunsInfo for a particular schedule
   */
  @GET
  @Path("/schedules/{scheduleid}/runs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public List<String> getScheduleRuns(
      @QueryParam("sessionid") GrillSessionHandle sessionHandle,
      @PathParam("scheduleid") String scheduleid) {
    checkSessionId(sessionHandle);
    try {
      return schedulerService.getScheduleRuns(sessionHandle, scheduleid);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Gives details about a particular run of a schedule
   * 
   * @param sessionid
   * @param scheduleid
   * @param duration
   * @return RunInfo for a particular schedule
   */
  @GET
  @Path("/schedules/{scheduleid}/runs/{runid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public ScheduleRunInfo getScheduleRunDetail(
      @QueryParam("sessionid") GrillSessionHandle sessionHandle,
      @PathParam("scheduleid") String scheduleid,
      @PathParam("runid") String runHandle) {
    checkSessionId(sessionHandle);
    try {
      return schedulerService.getScheduleRunDetail(sessionHandle, scheduleid,
          runHandle);
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
  @Path("/schedules")
  @Consumes({ MediaType.MULTIPART_FORM_DATA })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public String schedule(
      @QueryParam("sessionid") GrillSessionHandle sessionHandle,
      @FormDataParam("grillschedule") XSchedule schedule) {
    checkSessionId(sessionHandle);
    try {
      return schedulerService.scheduleTask(sessionHandle, schedule);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Edit an existing schedule Task
   * 
   * @param sessionid
   * @param scheduleid
   * @param newGrillSchedule
   * 
   * @return boolean depending on update successful
   */
  @PUT
  @Path("/schedules/{scheduleid}")
  @Consumes({ MediaType.MULTIPART_FORM_DATA })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public boolean updateTask(
      @QueryParam("sessionid") GrillSessionHandle sessionHandle,
      @PathParam("scheduleid") String scheduleid,
      @QueryParam("newgrillschedule") XSchedule newSchedule) {
    checkSessionId(sessionHandle);
    try {
      return schedulerService.updateSchedule(sessionHandle, scheduleid,
          newSchedule);
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
  @Path("/schedules/{scheduleid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public void statusUpdate(@PathParam("scheduleid") String scheduleid,
      @QueryParam("sessionid") GrillSessionHandle sessionHandle,
      @QueryParam("newstatus") String newstatus) {
    checkSessionId(sessionHandle);
    try {
      schedulerService
          .updateStatus(sessionHandle, scheduleid, newstatus);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  @POST
  @Path("/schedules/{scheduleid}/runs/{runid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public boolean rerun(@PathParam("scheduleid") String scheduleid,
      @PathParam("runid") String runid,
      @QueryParam("sessionid") GrillSessionHandle sessionHandle) {
    checkSessionId(sessionHandle);
    try {
      return schedulerService.rerun(sessionHandle, scheduleid, runid);
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
  @Path("/schedules/{scheduleid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,
  MediaType.TEXT_PLAIN })
  public boolean deleteSchedule(
      @QueryParam("sessionid") GrillSessionHandle sessionHandle,
      @PathParam("scheduleid") String scheduleid) {
    checkSessionId(sessionHandle);
    try {
      return schedulerService.delete(sessionHandle, scheduleid);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

}
