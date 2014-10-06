package com.inmobi.grill.server.query.rewrite;

import com.inmobi.grill.driver.cube.MockDriver;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.query.rewrite.QueryCommand;
import com.inmobi.grill.server.query.rewrite.dsl.DSLRegistry;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@PrepareForTest({CubeQLCommandImpl.class, DriverSpecificQueryRewriterImpl.class})
@PowerMockIgnore({"org.apache.log4j.*", "javax.management.*", "javax.xml.*",
    "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*", "org.w3c.dom*"})
public class TestDriverSpecificRewriter {

  DriverSpecificQueryRewrite rewriter;
  List<GrillDriver> drivers = new ArrayList<GrillDriver>();
  HiveConf conf = new HiveConf();
  MockDriver driver = new MockDriver();
  DSLRegistry registry;

  /**
   * We need a special {@link org.testng.IObjectFactory}.
   *
   * @return {@link org.powermock.modules.testng.PowerMockObjectFactory}.
   */
  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new org.powermock.modules.testng.PowerMockObjectFactory();
  }

  @BeforeTest
  public void beforeTest() throws Exception {
    driver.configure(conf);
    drivers.add(driver);
    conf.set(GrillConfConstants.GRILL_QUERY_REWRITER, "com.inmobi.grill.server.query.rewrite.DriverSpecificQueryRewriterImpl");
    conf.set(GrillConfConstants.GRILL_QUERY_DSLS, "test");
    conf.set("grill.query.test.dsl.impl", "com.inmobi.grill.server.query.rewrite.dsl.TestDSL");
    registry = DSLRegistry.getInstance();

  }


  @Test
  public void testNonSQLRewrite() throws Exception {
    rewriter = new DriverSpecificQueryRewriterImpl();
    final String TEST_COMMAND="add test.jar";
    final QueryCommand queryCommand = QueryCommands.get(TEST_COMMAND, null, conf);
    Assert.assertEquals(queryCommand.getType(), QueryCommand.Type.NONSQL);

    final Map<GrillDriver, QueryCommand> rewrittenQuery = rewriter.rewrite(queryCommand, drivers);
    Assert.assertEquals(rewrittenQuery.get(driver).getCommand(), TEST_COMMAND);
  }

  @Test
  public void testDSLRewrite() throws Exception {

    CubeQueryRewriter mockWriter = TestRewriting.getMockedRewriter();
    final ClassLoader mockClassLoader = mockWriter.getClass().getClassLoader();
    PowerMockito.stub(PowerMockito.method(CubeQLCommandImpl.class, "getRewriter")).toReturn(mockWriter);
    registry.init(conf, mockClassLoader);
    rewriter = RewriteUtil.getQueryRewriter(conf, mockClassLoader);

    final String TEST_COMMAND="domain select * from domain_entity";
    final String REWRITTEN_COMMAND="select name from table";
    final QueryCommand queryCommand = QueryCommands.get(TEST_COMMAND, null, conf);
    Assert.assertEquals(queryCommand.getType(), QueryCommand.Type.DOMAIN);

    final Map<GrillDriver, QueryCommand> rewrittenQuery = rewriter.rewrite(queryCommand, drivers);
    Assert.assertEquals(StringUtils.trim(rewrittenQuery.get(driver).getCommand()), REWRITTEN_COMMAND);
  }

  @Test
  public void testCubeRewrite() throws Exception {

    CubeQueryRewriter mockWriter = TestRewriting.getMockedRewriter();
    final ClassLoader mockClassLoader = mockWriter.getClass().getClassLoader();
    PowerMockito.stub(PowerMockito.method(CubeQLCommandImpl.class, "getRewriter")).toReturn(mockWriter);
    registry.init(conf, mockClassLoader);
    rewriter = RewriteUtil.getQueryRewriter(conf, mockClassLoader);

    final String TEST_COMMAND="cube select * from table";
    final String REWRITTEN_COMMAND="select * from table";
    final QueryCommand queryCommand = QueryCommands.get(TEST_COMMAND, null, conf);
    Assert.assertEquals(queryCommand.getType(), QueryCommand.Type.CUBE);

    final Map<GrillDriver, QueryCommand> rewrittenQuery = rewriter.rewrite(queryCommand, drivers);
    Assert.assertEquals(StringUtils.trim(rewrittenQuery.get(driver).getCommand()), REWRITTEN_COMMAND);
  }

  /**
   *  HQL rewrite is a No-op.
   *
   */

  @Test
  public void testHQLRewrite() throws Exception {
    rewriter = new DriverSpecificQueryRewriterImpl();
    final String TEST_COMMAND="select * from table";
    final String REWRITTEN_COMMAND="select * from table";
    final QueryCommand queryCommand = QueryCommands.get(TEST_COMMAND, null, conf);
    Assert.assertEquals(queryCommand.getType(), QueryCommand.Type.HQL);

    final Map<GrillDriver, QueryCommand> rewrittenQuery = rewriter.rewrite(queryCommand, drivers);
    Assert.assertEquals(rewrittenQuery.get(driver).getCommand(), REWRITTEN_COMMAND);
  }

}
