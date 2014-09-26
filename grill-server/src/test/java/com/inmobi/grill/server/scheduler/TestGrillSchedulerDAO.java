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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.TimeZone;

import javax.ws.rs.core.Application;
import javax.xml.datatype.Duration;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.namespace.QName;

import org.testng.Assert;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.schedule.XExecution;
import com.inmobi.grill.api.schedule.XSchedule;
import com.inmobi.grill.api.schedule.XScheduleQuery;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.scheduler.GrillScheduleRunInfo;
import com.inmobi.grill.server.api.scheduler.SchedulerService;
import com.inmobi.grill.server.query.QueryApp;

public class TestGrillSchedulerDAO extends GrillJerseyTest {
  GrillSchedulerDAO dao;
  SchedulerServiceImpl service;

  @BeforeClass
  public void setup() {
    Configuration conf = new Configuration();
    dao = new GrillSchedulerDAO();
    dao.init(conf);
    service =
        (SchedulerServiceImpl) GrillServices.get().getService(
            SchedulerService.NAME);
  }

  @Test
  public void testGrillServerDAO() throws Exception {
    XSchedule schedule = new XSchedule();

    // Test insert query for schedule_info

    String uuid = service.grillSchedulerDao.insertScheduleInfo(schedule, "foo");
    Assert.assertEquals(uuid, service.grillSchedulerDao
        .getScheduleDetails(uuid).getScheduleHandle());

    GrillSessionHandle session =
        service.openSession("foo@localhost", "bar",
            new HashMap<String, String>());
    GrillScheduleRunInfo runInfo = new GrillScheduleRunInfo();
    service.grillSchedulerDao.insertScheduleRunInfo(runInfo);

    // Test find finished queries
  }

  @Override
  protected int getTestPort() {
    return 101011;
  }

  @Override
  protected Application configure() {
    return new QueryApp();
  }
}
