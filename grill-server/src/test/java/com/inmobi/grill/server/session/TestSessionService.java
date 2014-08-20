package com.inmobi.grill.server.session;

import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.GrillServices;
import org.testng.annotations.Test;

import java.util.HashMap;

/**
 * User: rajat.khandelwal
 * Date: 20/08/14
 */
@Test(groups="unit-test")
public class TestSessionService  extends GrillJerseyTest {
  @Test
  public void testWrongAuth() throws Exception{
    HiveSessionService sessionService = GrillServices.get().getService("session");
    sessionService.openSession("a","b", new HashMap<String, String>());
  }

  @Override
  protected int getTestPort() {
    return 8082;
  }
}
