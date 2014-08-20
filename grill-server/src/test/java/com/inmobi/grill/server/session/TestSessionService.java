package com.inmobi.grill.server.session;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.auth.FooBarAuthenticationProvider;
import com.inmobi.grill.server.metastore.MetastoreApp;
import org.apache.hive.service.Service;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;
import java.util.HashMap;

/**
 * User: rajat.khandelwal
 * Date: 20/08/14
 */
@Test(groups="unit-test")
public class TestSessionService  extends GrillJerseyTest {
  private HiveSessionService sessionService;

  @BeforeTest
  public void init(){
    sessionService = GrillServices.get().getService("session");
  }
  @Test
  public void testWrongAuth() {
    try{
      sessionService.openSession("a","b", new HashMap<String, String>());
    } catch (GrillException e) {
      Assert.assertEquals(e.getCause().getCause().getMessage(), FooBarAuthenticationProvider.MSG);
    }
  }

  @Test
  public void testCorrectAuth() {
    try{
      sessionService.openSession("foo","bar", new HashMap<String, String>());
    } catch (GrillException e) {
      Assert.fail("Shouldn't have thrown exception");
    }
  }

  @Override
  protected int getTestPort() {
    return 8082;
  }

  @Override
  protected Application configure() {
    return new SessionApp();
  }
}
