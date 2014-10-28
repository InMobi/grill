/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query.rewrite;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import static org.mockito.Matchers.any;

import org.apache.lens.api.LensException;
import org.apache.lens.driver.cube.MockDriver;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.query.rewrite.dsl.TestDSL;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockObjectFactory;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;

/**
 * The Class TestRewriting.
 */
@PrepareForTest(CubeQueryRewriter.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*", "javax.xml.*",
    "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*", "org.w3c.dom*" })
public class TestRewriting {

  HiveConf conf = new HiveConf();
  DriverSpecificQueryRewrite rewriter = null;

  @BeforeTest
  public void beforeTest() throws Exception {
    conf.setInt("mock.driver.test.val", 5);
    conf.set(LensConfConstants.QUERY_REWRITER, DriverSpecificQueryRewriterImpl.class.getCanonicalName());
    conf.set(LensConfConstants.QUERY_DSLS, "test");
    conf.set("grill.query.test.dsl.impl", TestDSL.class.getCanonicalName());
  }

  @BeforeTest
  public void afterTest() throws Exception {
  }
  /**
   * We need a special {@link IObjectFactory}.
   *
   * @return {@link PowerMockObjectFactory}.
   */
  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new org.powermock.modules.testng.PowerMockObjectFactory();
  }

  static CubeQueryRewriter getMockedRewriter() throws SemanticException, ParseException {
    CubeQueryRewriter mockwriter = Mockito.mock(CubeQueryRewriter.class);
    Mockito.when(mockwriter.rewrite(any(String.class))).thenAnswer(new Answer<CubeQueryContext>() {
      @Override
      public CubeQueryContext answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        return getMockedCubeContext((String) args[0]);
      }
    });
    Mockito.when(mockwriter.rewrite(any(ASTNode.class))).thenAnswer(new Answer<CubeQueryContext>() {
      @Override
      public CubeQueryContext answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        return getMockedCubeContext((ASTNode) args[0]);
      }
    });
    return mockwriter;
  }

  /**
   * Gets the mocked cube context.
   *
   * @param query
   *          the query
   * @return the mocked cube context
   * @throws SemanticException
   *           the semantic exception
   * @throws ParseException
   *           the parse exception
   */
  static CubeQueryContext getMockedCubeContext(String query) throws SemanticException, ParseException {
    CubeQueryContext context = Mockito.mock(CubeQueryContext.class);
    Mockito.when(context.toHQL()).thenReturn(query.substring(4));
    Mockito.when(context.toAST(any(Context.class))).thenReturn(HQLParser.parseHQL(query.substring(4)));
    return context;
  }

  /**
   * Gets the mocked cube context.
   *
   * @param ast
   *          the ast
   * @return the mocked cube context
   * @throws SemanticException
   *           the semantic exception
   * @throws ParseException
   *           the parse exception
   */
  static CubeQueryContext getMockedCubeContext(ASTNode ast) throws SemanticException, ParseException {
    CubeQueryContext context = Mockito.mock(CubeQueryContext.class);
    if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
      if (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        // remove cube child from AST
        for (int i = 0; i < ast.getChildCount() - 1; i++) {
          ast.setChild(i, ast.getChild(i + 1));
        }
        ast.deleteChild(ast.getChildCount() - 1);
      }
    }
    StringBuilder builder = new StringBuilder();
    HQLParser.toInfixString(ast, builder);
    Mockito.when(context.toHQL()).thenReturn(builder.toString());
    Mockito.when(context.toAST(any(Context.class))).thenReturn(ast);
    return context;
  }

  /**
   * Test cube query.
   *
   * @throws ParseException
   *           the parse exception
   * @throws SemanticException
   *           the semantic exception
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testCubeQuery() throws ParseException, SemanticException, LensException {
    List<LensDriver> drivers = new ArrayList<LensDriver>();
    MockDriver driver = new MockDriver();
    driver.configure(conf);
    drivers.add(driver);

    CubeQueryRewriter mockWriter = getMockedRewriter();
    final ClassLoader mockClassLoader = mockWriter.getClass().getClassLoader();
    PowerMockito.stub(PowerMockito.method(CubeQLCommandImpl.class, "getRewriter")).toReturn(mockWriter);
    rewriter = RewriteUtil.getQueryRewriter(conf, mockClassLoader);

    String q1 = "select name from table";
    Assert.assertFalse(CubeQLCommandImpl.isCubeQuery(q1));
    List<CubeQLCommandImpl.CubeQueryInfo> cubeQueries = CubeQLCommandImpl.findCubePositions(q1);
    Assert.assertEquals(cubeQueries.size(), 0);
    RewriteUtil.rewrite(rewriter, q1, null, conf, drivers);

    String q2 = "cube select name from table";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "insert overwrite directory '/tmp/rewrite' cube select name from table";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "insert overwrite local directory '/tmp/rewrite' cube select name from table";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "insert overwrite local directory '/tmp/example-output' " +
        "cube select id,name from dim_table";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select id,name from dim_table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "explain cube select name from table";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (cube select name from table) a";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "insert overwrite directory '/tmp/rewrite' " +
        "select * from (cube select name from table) a";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (cube select name from table)a";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from  (  cube select name from table   )     a";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (      cube select name from table where" +
        " (name = 'ABC'||name = 'XYZ')&&(key=100)   )       a";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(QueryCommands.preProcessQuery(q2));
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from" +
        " table where (name = 'ABC' OR name = 'XYZ') AND (key=100)");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (cube select name from table) a join (cube select" +
        " name2 from table2) b";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (cube select name from table) a full outer join" +
        " (cube select name2 from table2) b on a.name=b.name2";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);

    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (cube select name from table) a join (select name2 from table2) b";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (cube select name from table union all cube select name2 from table2) u";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "insert overwrite directory '/tmp/rewrite' select * from (cube select name from table " +
        "union all cube select name2 from table2) u";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select u.* from (select name from table    union all       " +
        "cube select name2 from table2)   u";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name2 from table2");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select u.* from (select name from table union all cube select name2 from table2)u";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name2 from table2");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (cube select name from table union all cube select name2" +
        " from table2 union all cube select name3 from table3) u";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    Assert.assertEquals(cubeQueries.size(), 3);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    Assert.assertEquals(cubeQueries.get(2).query, "cube select name3 from table3");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    //multiple whietspace betwween cube and select
    q2 = "select * from   (     cube          select name from table    union all   cube" +
        " select name2 from table2   union all  cube select name3 from table3 )  u";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    Assert.assertEquals(cubeQueries.size(), 3);
    Assert.assertEquals(cubeQueries.get(0).query, "cube          select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    Assert.assertEquals(cubeQueries.get(2).query, "cube select name3 from table3");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (cube select name from table union all cube select" +
        " name2 from table2) u group by u.name";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "select * from (cube select name from table union all cube select" +
        " name2 from table2)  u group by u.name";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "create table temp1 as cube select name from table";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);
    Assert.assertEquals(cubeQueries.size(), 1);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "create table temp1 as select * from (cube select name from table union all cube select" +
        " name2 from table2)  u group by u.name";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);
    Assert.assertEquals(cubeQueries.size(), 2);
    Assert.assertEquals(cubeQueries.get(0).query, "cube select name from table");
    Assert.assertEquals(cubeQueries.get(1).query, "cube select name2 from table2");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);

    q2 = "create table temp1 as cube select name from table where" +
        " time_range_in('dt', '2014-06-24-23', '2014-06-25-00')";
    Assert.assertTrue(CubeQLCommandImpl.isCubeQuery(q2));
    cubeQueries = CubeQLCommandImpl.findCubePositions(q2);
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);
    Assert.assertEquals(cubeQueries.size(), 1);

    Assert.assertEquals(cubeQueries.get(0).query,
        "cube select name from table where time_range_in('dt', '2014-06-24-23', '2014-06-25-00')");
    RewriteUtil.rewrite(rewriter, q2, null, conf, drivers);
  }
}
