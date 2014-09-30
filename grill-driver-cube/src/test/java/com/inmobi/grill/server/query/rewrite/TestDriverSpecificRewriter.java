package com.inmobi.grill.server.query.rewrite;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.driver.cube.MockDriver;
import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.query.rewrite.QueryCommand;
import com.inmobi.grill.server.query.rewrite.dsl.DSLRegistry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestDriverSpecificRewriter {

  DriverSpecificQueryRewrite rewriter;
  List<GrillDriver> drivers = new ArrayList<GrillDriver>();
  HiveConf conf = new HiveConf();
  MockDriver driver = new MockDriver();
  DSLRegistry registry;

  @BeforeTest
  public void beforeTest() throws Exception {
    rewriter = new DriverSpecificQueryRewriterImpl();
    driver.configure(conf);
    drivers.add(driver);
    conf.set("grill.server.query.rewriter", "com.inmobi.grill.server.query.rewrite.DriverSpecificQueryRewriterImpl");
    conf.set("grill.query.dsls", "com.inmobi.grill.server.query.rewrite.dsl.TestDSL");
    registry = DSLRegistry.getInstance();
    registry.init(conf);
  }

  @AfterTest
  public void afterTest() throws Exception {
  }

  @Test
  public void testNonSQLRewrite() throws Exception {
    final String TEST_COMMAND="add test.jar";
    final QueryCommand queryCommand = QueryCommands.get(TEST_COMMAND, null, conf);
    Assert.assertEquals(queryCommand.getType(), QueryCommand.Type.NONSQL);

    final Map<GrillDriver, QueryCommand> rewrittenQuery = rewriter.rewrite(queryCommand, drivers);
    Assert.assertEquals(rewrittenQuery.get(driver).getCommand(), TEST_COMMAND);
  }

  @Test
  public void testDSLRewrite() throws Exception {
    final String TEST_COMMAND="domain select * from domain_entity";
    final String REWRITTEN_COMMAND="cube select name from table";
    final QueryCommand queryCommand = QueryCommands.get(TEST_COMMAND, null, conf);
    Assert.assertEquals(queryCommand.getType(), QueryCommand.Type.DOMAIN);

    final Map<GrillDriver, QueryCommand> rewrittenQuery = rewriter.rewrite(queryCommand, drivers);
    Assert.assertEquals(rewrittenQuery.get(driver).getCommand(), REWRITTEN_COMMAND);
  }

  @Test
  public void testInvalidCommand() throws Exception {
    final String TEST_COMMAND="check invalid";
    final QueryCommand queryCommand = QueryCommands.get(TEST_COMMAND, null, conf);
    Assert.assertEquals(queryCommand.getType(), QueryCommand.Type.HQL);

    try {
      rewriter.rewrite(queryCommand, drivers);
    } catch(GrillException pe) {
      Assert.assertTrue(pe.getCause().getClass().equals(ParseException.class));
    }
    Assert.fail("Expected ParseException for invalid command " + TEST_COMMAND);
  }

  @Test
  public void testCubeRewrite() throws Exception {
    final String TEST_COMMAND="cube select * from table";
    final String REWRITTEN_COMMAND="select * from cube_table";
    final QueryCommand queryCommand = QueryCommands.get(TEST_COMMAND, null, conf);
    Assert.assertEquals(queryCommand.getType(), QueryCommand.Type.CUBE);

    final Map<GrillDriver, QueryCommand> rewrittenQuery = rewriter.rewrite(queryCommand, drivers);
    Assert.assertEquals(rewrittenQuery.get(driver).getCommand(), REWRITTEN_COMMAND);
  }

  /**
   *  HQL rewrite is a No-op.
   *
   */

  @Test
  public void testHQLRewrite() throws Exception {
    final String TEST_COMMAND="select * from table";
    final String REWRITTEN_COMMAND="select * from table";
    final QueryCommand queryCommand = QueryCommands.get(TEST_COMMAND, null, conf);
    Assert.assertEquals(queryCommand.getType(), QueryCommand.Type.CUBE);

    final Map<GrillDriver, QueryCommand> rewrittenQuery = rewriter.rewrite(queryCommand, drivers);
    Assert.assertEquals(rewrittenQuery.get(driver).getCommand(), REWRITTEN_COMMAND);
  }

}
