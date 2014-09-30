package com.inmobi.grill.server.query.rewrite.dsl;

import com.inmobi.grill.server.api.query.rewrite.QueryCommand;
import com.inmobi.grill.server.api.query.rewrite.dsl.DSLCommand;
import com.inmobi.grill.server.api.query.rewrite.dsl.DSLSemanticException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestDSLCommand {


  HiveConf conf = new HiveConf();
  DSLRegistry registry;

  @BeforeTest
  public void beforeTest() throws Exception {
    conf.set("grill.server.query.rewriter", "com.inmobi.grill.server.query.rewrite.DriverSpecificQueryRewriterImpl");
    registry = DSLRegistry.getInstance();
    registry.init(conf);
  }

  @AfterTest
  public void afterTest() throws Exception {
  }

  @Test
  public void testDSLValidCommand() throws Exception {
    final String TEST_COMMAND="domain select * from domain_entity";
    final String REWRITTEN_COMMAND="cube select name from table";
    DSLCommand command = new DSLCommandImpl(TEST_COMMAND, null, conf);

    Assert.assertTrue(command.matches(TEST_COMMAND));

    final QueryCommand rewrittenQuery = command.rewrite();
    Assert.assertEquals(rewrittenQuery.getCommand(), REWRITTEN_COMMAND);
  }

  @Test
  public void testDSLInValidCommand() throws Exception {

    final String TEST_COMMAND="select * from domain_entity";
    DSLCommand command = new DSLCommandImpl(TEST_COMMAND, null, conf);

    Assert.assertFalse(command.matches("check invalid"));
    try {
      command.rewrite();
    }catch(DSLSemanticException dse) {
    }
    Assert.fail("Expected DSlSemanticException during query rewrite!");
  }
}
