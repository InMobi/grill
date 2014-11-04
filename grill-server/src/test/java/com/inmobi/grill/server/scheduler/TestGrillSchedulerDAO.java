//package com.inmobi.grill.server.scheduler;
//
///*
// * #%L
// * Grill Server
// * %%
// * Copyright (C) 2014 Inmobi
// * %%
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * 
// *      http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * #L%
// */
//
//import java.util.Date;
//import java.util.GregorianCalendar;
//
//import javax.ws.rs.core.Application;
//import javax.xml.datatype.DatatypeFactory;
//import javax.xml.datatype.XMLGregorianCalendar;
//
//import org.testng.Assert;
//import org.testng.annotations.Test;
//
//import com.inmobi.grill.api.schedule.XExecution;
//import com.inmobi.grill.api.schedule.XFrequency;
//import com.inmobi.grill.api.schedule.XFrequencyType;
//import com.inmobi.grill.api.schedule.XSchedule;
//import com.inmobi.grill.api.schedule.XScheduleQuery;
//import com.inmobi.grill.api.schedule.XStartSpec;
//import com.inmobi.grill.server.GrillJerseyTest;
//import com.inmobi.grill.server.GrillServices;
//import com.inmobi.grill.server.api.scheduler.SchedulerService;
//
//public class TestGrillSchedulerDAO extends GrillJerseyTest {
//
//  @SuppressWarnings("deprecation")
//  @Test
//  public void testGrillServerDAO() throws Exception {
//    SchedulerServiceImpl service =
//        (SchedulerServiceImpl) GrillServices.get().getService(
//            SchedulerService.NAME);
//
//    XSchedule schedule = new XSchedule();
//    GregorianCalendar start = new GregorianCalendar();
//    start.setTime(new Date());
//    GregorianCalendar end = new GregorianCalendar();
//    Date endDate = new Date();
//    endDate.setYear(2099);
//    XMLGregorianCalendar startdate =
//        DatatypeFactory.newInstance().newXMLGregorianCalendar(start);
//    XMLGregorianCalendar enddate =
//        DatatypeFactory.newInstance().newXMLGregorianCalendar(end);
//
//    XExecution execution = new XExecution();
//    XScheduleQuery scheduleQuery = new XScheduleQuery();
//    scheduleQuery.setQuery("SELECT ID from testTable;");
//    scheduleQuery.setSessionDb("");
//    execution.setQueryType(scheduleQuery);
//    schedule.setExecution(execution);
//
//    XStartSpec startSpec = new XStartSpec();
//    XFrequency frequency = new XFrequency();
//    frequency.setFrequncyEnum(XFrequencyType.DAILY);
//    startSpec.setFrequency(frequency);
//    schedule.setStartSpec(startSpec);
//    schedule.setStartTime(startdate);
//    schedule.setEndTime(enddate);
//
//    // Test insert query for schedule_info
//    String uuid = service.grillSchedulerDao.insertScheduleInfo(schedule, "foo");
//    Assert.assertEquals(uuid, service.grillSchedulerDao
//        .getScheduleDetails(uuid).getScheduleHandle());
//
//    // GrillSessionHandle session =
//    // service.openSession("foo@localhost", "bar",
//    // new HashMap<String, String>());
//    // GrillScheduleRunInfo runInfo = new GrillScheduleRunInfo();
//    // runInfo.setScheduleId(uuid);
//    // runInfo.setSessionHandle(UUID.randomUUID().toString());
//    // runInfo.setRunHandle("test");
//    // service.grillSchedulerDao.insertScheduleRunInfo(runInfo);
//
//    // Test find finished queries
//  }
//
//  @Override
//  protected int getTestPort() {
//    return 101011;
//  }
//
//  @Override
//  protected Application configure() {
//    return new SchedulerApp();
//  }
//}
