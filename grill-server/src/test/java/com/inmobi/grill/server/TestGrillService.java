package com.inmobi.grill.server;

import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.session.GrillSessionImpl;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestGrillService {
  class MockGrillService extends GrillService {
    protected MockGrillService(String name, CLIService cliService) {
      super(name, cliService);
      addService(cliService);
    }
  }


  @Test
  public void testSessionExpiry() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, GrillSessionImpl.class.getName());
    conf.setLong(GrillConfConstants.GRILL_SESSION_TIMEOUT_SECONDS, 1L);
    CLIService cliService = new CLIService();
    MockGrillService grillService = new MockGrillService("MOCK_GRILL_SERVICE", cliService);
    grillService.init(conf);
    grillService.start();

    try {
      GrillSessionHandle sessionHandle =
        grillService.openSession("test", "test", new HashMap<String, String>());
      GrillSessionImpl session = grillService.getSession(sessionHandle);
      assertTrue(session.isActive());
      Thread.sleep(3000);
      assertFalse(session.isActive());
      try {
        grillService.getSession(sessionHandle);
        // should throw exception since session should be expired by now
        fail("Expected get session to fail for session " + sessionHandle.getPublicId());
      } catch (Exception e) {
        // pass
      }
    } finally {
      grillService.stop();
    }
  }

}
