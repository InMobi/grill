package com.inmobi.grill.server.scheduler;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

import com.google.gson.Gson;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.schedule.MapType;
import com.inmobi.grill.api.schedule.ScheduleStatus.Status;
import com.inmobi.grill.api.schedule.XFrequency;
import com.inmobi.grill.api.schedule.XFrequencyType;
import com.inmobi.grill.api.schedule.XSchedule;
import com.inmobi.grill.api.schedule.XStartSpec;

public class ScheduleJob {

  private static final Log LOG = LogFactory.getLog(ScheduleJob.class);
  private static Properties prop = new Properties();
  private Gson gson = new Gson();
  static {
    prop.setProperty("org.quartz.scheduler.instanceName", "GRILL_JOB_SCHEDULER");
    prop.setProperty("org.quartz.threadPool.class",
        "org.quartz.simpl.SimpleThreadPool");
    prop.setProperty("org.quartz.threadPool.threadCount", "4");
    prop.setProperty(
        "org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread",
        "true");
    prop.setProperty("org.quartz.jobStore.class",
        "org.quartz.impl.jdbcjobstore.JobStoreTX");
    prop.setProperty("org.quartz.jobStore.driverDelegateClass",
        "org.quartz.impl.jdbcjobstore.StdJDBCDelegate");
    prop.setProperty("org.quartz.jobStore.dataSource", "tasksDataStore");
    prop.setProperty("org.quartz.jobStore.tablePrefix", "QRTZ_");
    prop.setProperty("org.quartz.jobStore.misfireThreshold", "60000");
    prop.setProperty("org.quartz.jobStore.isClustered", "false");
    prop.setProperty("org.quartz.dataSource.tasksDataStore.driver", "");
    prop.setProperty("org.quartz.dataSource.tasksDataStore.URL", "");
    prop.setProperty("org.quartz.dataSource.tasksDataStore.user", "");
    prop.setProperty("org.quartz.dataSource.tasksDataStore.password", "");
    prop.setProperty("org.quartz.dataSource.tasksDataStore.maxConnections",
        "20");
  }

  public ScheduleJob() {
  }

  public ScheduleJob(XSchedule s, Status status, String scheduleid) {
    if (status.equals(Status.SCHEDULED)) {
      if (s.getExecution().getQueryType() != null) {
        // get all the objects from schedule
        try {
          schedule(s, scheduleid);
        } catch (SchedulerException e) {
          LOG.error("Unable to schedule Job.", e);
        }
      }
    } else { // means status is Paused, stop the schedule; Will delete the
             // schedule entry from quartz for this.
      try {
        deschedule(scheduleid);
      } catch (SchedulerException e) {
        LOG.error("Unable to schedule Job.", e);
      }
    }
  }

  /**
   * Deletes a schedule Job and trigger from quartz for specific scheduleId.
   * 
   * @param XSchedule
   * @throws SchedulerException
   */
  public void deschedule(String scheduleid) throws SchedulerException {
    SchedulerFactory sf = new StdSchedulerFactory(prop);
    Scheduler scheduler = sf.getScheduler();
    scheduler.unscheduleJob(new TriggerKey(scheduleid));
    scheduler.deleteJob(new JobKey(scheduleid));
  }

  /**
   * Schedules a Job with frequncy params as trigger using quartz.
   * 
   * @param XSchedule
   * @param scheduleid
   * @throws SchedulerException
   */
  public void schedule(XSchedule s, String scheduleid)
      throws SchedulerException {
    SchedulerFactory sf = new StdSchedulerFactory(prop);
    Scheduler sched = sf.getScheduler();
    Trigger trigger = null;

    TimeZone timeZone = TimeZone.getDefault();
    Date start = new Date(s.getStartTime().getMillisecond());
    Date end = new Date(s.getEndTime().getMillisecond());

    String resource_Path = gson.toJson(s.getResourcePath());
    XStartSpec startSpec = s.getStartSpec();
    if (startSpec.getId() != null) {
      // handle scheduleId dependency
    } else {
      XFrequency frequency = startSpec.getFrequency();
      if (frequency.getFrequncyEnum() != null) {
        switch (XFrequencyType.valueOf(frequency.getFrequncyEnum().toString())) {
        case DAILY:
          trigger =
              newTrigger()
                  .withIdentity(new TriggerKey(scheduleid))
                  .withSchedule(
                      cronSchedule("0 0 12 * * ?").inTimeZone(timeZone))
                  .startAt(start).endAt(end).build();
          break;
        case WEEKLY:
          trigger =
              newTrigger()
                  .withIdentity(new TriggerKey(scheduleid))
                  .withSchedule(
                      cronSchedule("0 0 12 ? * MON").inTimeZone(timeZone))
                  .startAt(start).endAt(end).build();
          break;
        case MONTHLY:
          trigger =
              newTrigger()
                  .withIdentity(new TriggerKey(scheduleid))
                  .withSchedule(
                      cronSchedule("0 0 12 1 * ?").inTimeZone(timeZone))
                  .startAt(start).endAt(end).build();
          break;
        case QUARTERLY:
          trigger =
              newTrigger()
                  .withIdentity(new TriggerKey(scheduleid))
                  .withSchedule(
                      cronSchedule("0 0 12 1 1,4,7,10 ?").inTimeZone(timeZone))
                  .startAt(start).endAt(end).build();
          break;
        case YEARLY:
          trigger =
              newTrigger()
                  .withIdentity(new TriggerKey(scheduleid))
                  .withSchedule(
                      cronSchedule("0 0 12 1 1 ? *").inTimeZone(timeZone))
                  .startAt(start).endAt(end).build();
          break;
        }
      } else {
        trigger =
            newTrigger()
                .withIdentity(new TriggerKey(scheduleid))
                .withSchedule(
                    cronSchedule(frequency.getCronExpression()).inTimeZone(
                        timeZone)).startAt(start).endAt(end).build();
      }
    }

    if (s.getExecution().getQueryType() != null) {
      String query = s.getExecution().getQueryType().getQuery();
      String session_db = s.getExecution().getQueryType().getSessionDb();
      GrillConf grillConf = new GrillConf();
      for (MapType conf : s.getExecution().getQueryType().getQueryConf()) {
        grillConf.addProperty(conf.getKey(), conf.getValue());
      }
      for (MapType conf : s.getExecution().getQueryType().getSessionConf()) {
        grillConf.addProperty(conf.getKey(), conf.getValue());
      }
      for (MapType conf : s.getScheduleConf()) {
        grillConf.addProperty(conf.getKey(), conf.getValue());
      }
      String conf = gson.toJson(grillConf);

      sched.start();
      JobDetail job =
          newJob(ScheduleQueryExecution.class)
              .withIdentity(new JobKey(scheduleid))
              .usingJobData("query", query)
              .usingJobData("scheduleid", scheduleid)
              .usingJobData("resource_path", resource_Path)
              .usingJobData("session_db", session_db)
              .usingJobData("conf", conf).build();
      sched.scheduleJob(job, trigger);
    }
  }
}
