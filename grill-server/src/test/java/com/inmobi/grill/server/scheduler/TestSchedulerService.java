package com.inmobi.grill.server.scheduler;

import static org.testng.Assert.fail;

import java.util.HashMap;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.BasicConfigurator;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;

@Test(groups = "unit-test")
public class TestSchedulerService extends GrillJerseyTest {
  SchedulerServiceImpl schedulerService;
  GrillSessionHandle grillSessionId;
  protected String mediaType = MediaType.APPLICATION_XML;
  protected MediaType medType = MediaType.APPLICATION_XML_TYPE;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    BasicConfigurator.configure();
    schedulerService =
        (SchedulerServiceImpl) GrillServices.get().getService("scheduler");
    grillSessionId =
        schedulerService.openSession("foo", "bar",
            new HashMap<String, String>());
  }

  @Override
  protected int getTestPort() {
    return 1050;
  }

  @AfterTest
  public void tearDown() throws Exception {
    schedulerService.closeSession(grillSessionId);
    super.tearDown();
  }

  @Override
  protected Application configure() {
    return new SchedulerApp();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  @Test
  public void testSetDatabase() throws Exception {
    WebTarget dbTarget = target().path("").path("databases/current");
    String dbName = "test_set_db";
    try {
      dbTarget.queryParam("sessionid", grillSessionId).request(mediaType).put(Entity.xml(dbName), APIResult.class);
      fail("Should get 404");
    } catch (NotFoundException e) {
      // expected
    }
  }
}
