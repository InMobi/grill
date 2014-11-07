package org.apache.lens.driver.hive;

/*
 * #%L
 * Grill Hive Driver
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

import static org.testng.Assert.*;

import java.io.*;
import java.util.*;

import junit.framework.Assert;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.lens.api.GrillException;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.ResultColumn;
import org.apache.lens.driver.hive.EmbeddedThriftConnection;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.driver.hive.HiveInMemoryResultSet;
import org.apache.lens.driver.hive.HivePersistentResultSet;
import org.apache.lens.driver.hive.HiveQueryPlan;
import org.apache.lens.driver.hive.ThriftConnection;
import org.apache.lens.server.api.GrillConfConstants;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.GrillResultSet;
import org.apache.lens.server.api.driver.GrillResultSetMetadata;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.testng.annotations.*;


public class TestHiveDriver {
  public static final String TEST_DATA_FILE = "testdata/testdata1.txt";
  public final String TEST_OUTPUT_DIR = "target/" + this.getClass().getSimpleName() + "/test-output";
  protected HiveConf conf = new HiveConf();
  protected HiveDriver driver;
  public final String DATA_BASE = this.getClass().getSimpleName();

  @BeforeTest
  public void beforeTest() throws Exception {
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));
    conf.addResource("hivedriver-site.xml");
    conf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS,
        EmbeddedThriftConnection.class, 
        ThriftConnection.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    SessionState ss = new SessionState(conf, "testuser");
    SessionState.start(ss);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestHiveDriver.class.getSimpleName());
    client.createDatabase(database, true);
    SessionState.get().setCurrentDatabase(TestHiveDriver.class.getSimpleName());

    driver = new HiveDriver();
    driver.configure(conf);
    conf.setBoolean(GrillConfConstants.GRILL_ADD_INSERT_OVEWRITE, false);
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    QueryContext context = new QueryContext(
        "USE " + TestHiveDriver.class.getSimpleName(), null, conf);
    driver.execute(context);
    conf.setBoolean(GrillConfConstants.GRILL_ADD_INSERT_OVEWRITE, true);
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    System.out.println("Driver created");
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  @AfterTest
  public void afterTest() throws Exception {
    driver.close();
    Hive.get(conf).dropDatabase(TestHiveDriver.class.getSimpleName(), true, true, true);
  }


  protected void createTestTable(String tableName) throws Exception {
    System.out.println("Hadoop Location: " + System.getProperty("hadoop.bin.path"));
    String createTable = "CREATE TABLE IF NOT EXISTS " + tableName  +"(ID STRING)" +
        " TBLPROPERTIES ('" + GrillConfConstants.STORAGE_COST + "'='500')";
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    // Craete again
    QueryContext context = new QueryContext(createTable, null, conf);
    GrillResultSet resultSet = driver.execute(context);
    assertNull(resultSet);

    // Load some data into the table
    String dataLoad = "LOAD DATA LOCAL INPATH '"+ TEST_DATA_FILE +"' OVERWRITE INTO TABLE " + tableName;
    context = new QueryContext(dataLoad, null, conf);
    resultSet = driver.execute(context);
    assertNull(resultSet);
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  // Tests
  @Test
  public void testInsertOverwriteConf() throws Exception {
    createTestTable("test_insert_overwrite");
    conf.setBoolean(GrillConfConstants.GRILL_ADD_INSERT_OVEWRITE, false);
    String query = "SELECT ID FROM test_insert_overwrite";
    QueryContext context = new QueryContext(query, null, conf);
    driver.addPersistentPath(context);
    assertEquals(context.getUserQuery(), query);
    assertNotNull(context.getDriverQuery());
    assertEquals(context.getDriverQuery(), context.getUserQuery());
  }

  @Test
  public void testTemptable() throws Exception {
    createTestTable("test_temp");
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    Hive.get(conf).dropTable("test_temp_output");
    String query = "CREATE TABLE test_temp_output AS SELECT ID FROM test_temp";
    QueryContext context = new QueryContext(query, null, conf);
    GrillResultSet resultSet = driver.execute(context);
    assertNull(resultSet);
    Assert.assertEquals(0, driver.getHiveHandleSize());

    // fetch results from temp table
    String select = "SELECT * FROM test_temp_output";
    context = new QueryContext(select, null, conf);
    resultSet = driver.execute(context);
    Assert.assertEquals(0, driver.getHiveHandleSize());
    validateInMemoryResult(resultSet, "test_temp_output");
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  @Test
  public void testExecuteQuery() throws Exception {
    createTestTable("test_execute");
    GrillResultSet resultSet = null;
    // Execute a select query
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    String select = "SELECT ID FROM test_execute";
    QueryContext context = new QueryContext(select, null, conf);
    resultSet = driver.execute(context);
    validateInMemoryResult(resultSet);
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    context = new QueryContext(select, null, conf);
    resultSet = driver.execute(context);
    validatePersistentResult(resultSet, TEST_DATA_FILE, context.getHDFSResultDir(), false);
    conf.set(GrillConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT,
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
            " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-'," +
        " 'field.delim'=','  ) STORED AS TEXTFILE ");
    select = "SELECT ID, null, ID FROM test_execute";
    context = new QueryContext(select, null, conf);
    resultSet = driver.execute(context);
    validatePersistentResult(resultSet, TEST_DATA_FILE, context.getHDFSResultDir(), true);
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  private void validateInMemoryResult(GrillResultSet resultSet) throws GrillException, IOException {
    validateInMemoryResult(resultSet, null);
  }

  private void validateInMemoryResult(GrillResultSet resultSet, String outputTable)
      throws GrillException, IOException {
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof HiveInMemoryResultSet);
    HiveInMemoryResultSet inmemrs = (HiveInMemoryResultSet) resultSet;

    // check metadata
    GrillResultSetMetadata rsMeta = inmemrs.getMetadata();
    List<ColumnDescriptor>  columns = rsMeta.getColumns();
    assertNotNull(columns);
    assertEquals(columns.size(), 1);
    String expectedCol = "";
    if (outputTable != null) {
      expectedCol += outputTable + ".";
    }
    expectedCol += "ID";
    assertTrue(columns.get(0).getName().toLowerCase().equals(expectedCol.toLowerCase())
        || columns.get(0).getName().toLowerCase().equals("ID".toLowerCase()));
    assertEquals(columns.get(0).getTypeName().toLowerCase(), "STRING".toLowerCase());

    List<String> expectedRows = new ArrayList<String>();
    // Read data from the test file into expectedRows
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(TEST_DATA_FILE)));
    String line = "";
    while ((line = br.readLine()) != null) {
      expectedRows.add(line.trim());
    }
    br.close();

    List<String> actualRows = new ArrayList<String>();
    while (inmemrs.hasNext()) {
      List<Object> row = inmemrs.next().getValues();
      actualRows.add((String)row.get(0));
    }
    assertEquals(actualRows, expectedRows);
  }

  public static class FailHook implements HiveDriverRunHook {

    @Override
    public void postDriverRun(HiveDriverRunHookContext arg0) throws Exception {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void preDriverRun(HiveDriverRunHookContext arg0) throws Exception {
      throw new GrillException("Failing this run");
    }
    
  }
  // executeAsync
  @Test
  public void testExecuteQueryAsync()  throws Exception {
    createTestTable("test_execute_sync");

    // Now run a command that would fail
    String expectFail = "SELECT ID FROM test_execute_sync";
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    conf.set("hive.exec.driver.run.hooks", FailHook.class.getCanonicalName());
    QueryContext context = new QueryContext(expectFail, null, conf);
    driver.executeAsync(context);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(context, DriverQueryState.FAILED, true, false);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    conf.set("hive.exec.driver.run.hooks", "");
    //  Async select query
    String select = "SELECT ID FROM test_execute_sync";
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    context = new QueryContext(select, null, conf);
    driver.executeAsync(context);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(context, DriverQueryState.SUCCESSFUL, false, false);
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    context = new QueryContext(select, null, conf);
    driver.executeAsync(context);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(context, DriverQueryState.SUCCESSFUL, true, false);
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    conf.set(GrillConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT,
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
            " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-'," +
        " 'field.delim'=','  ) STORED AS TEXTFILE ");
    select = "SELECT ID, null, ID FROM test_execute_sync";
    context = new QueryContext(select, null, conf);
    driver.executeAsync(context);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(context, DriverQueryState.SUCCESSFUL, true, true);
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  protected void validateExecuteAsync(QueryContext ctx, DriverQueryState finalState,
      boolean isPersistent, boolean formatNulls, HiveDriver driver) throws Exception {
    waitForAsyncQuery(ctx, driver);
    driver.updateStatus(ctx);
    assertEquals(ctx.getDriverStatus().getState(), finalState, "Expected query to finish with"
        + finalState);
    assertTrue(ctx.getDriverStatus().getDriverFinishTime() > 0);
    if (finalState.equals(DriverQueryState.SUCCESSFUL)) {
      System.out.println("Progress:" + ctx.getDriverStatus().getProgressMessage());
      assertNotNull(ctx.getDriverStatus().getProgressMessage());
      if (!isPersistent) {
        validateInMemoryResult(driver.fetchResultSet(ctx));
      } else{
        validatePersistentResult(driver.fetchResultSet(ctx), TEST_DATA_FILE, ctx.getHDFSResultDir(), formatNulls);
      }
    } else if (finalState.equals(DriverQueryState.FAILED)) {
      System.out.println("Error:" + ctx.getDriverStatus().getErrorMessage());
      System.out.println("Status:" + ctx.getDriverStatus().getStatusMessage());
      assertNotNull(ctx.getDriverStatus().getErrorMessage());
    }
  }

  protected void validateExecuteAsync(QueryContext ctx, DriverQueryState finalState,
      boolean isPersistent, boolean formatNulls) throws Exception {
    validateExecuteAsync(ctx, finalState, isPersistent, formatNulls, driver);
  }

  @Test
  public void testCancelAsyncQuery() throws Exception {
    createTestTable("test_cancel_async");
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    QueryContext context = new QueryContext("SELECT ID FROM test_cancel_async", null, conf);
    driver.executeAsync(context);
    driver.cancelQuery(context.getQueryHandle());
    driver.updateStatus(context);
    assertEquals(context.getDriverStatus().getState(), DriverQueryState.CANCELED, "Expecting query to be cancelled");
    driver.closeQuery(context.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    try {
      driver.cancelQuery(context.getQueryHandle());
      fail("Cancel on closed query should throw error");
    } catch (GrillException exc) {
      assertTrue(exc.getMessage().startsWith("Query not found"));
    }
  }

  private void validatePersistentResult(GrillResultSet resultSet, String dataFile, 
      Path outptuDir, boolean formatNulls) throws Exception {
    assertTrue(resultSet instanceof HivePersistentResultSet);
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();

    Path actualPath = new Path(path);
    FileSystem fs = actualPath.getFileSystem(conf);
    assertEquals(actualPath, fs.makeQualified(outptuDir));
    List<String> actualRows = new ArrayList<String>();
    for (FileStatus stat : fs.listStatus(actualPath)) {
      FSDataInputStream in = fs.open(stat.getPath());
      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(in));
        String line = "";

        while ((line = br.readLine()) != null) {
          System.out.println("Actual:" +line);
          actualRows.add(line.trim());
        }
      } finally {
        if (br != null) {
          br.close();
        }
      }
    }


    BufferedReader br = null;
    List<String> expectedRows = new ArrayList<String>();

    try {
      br = new BufferedReader(new FileReader(new File(dataFile)));
      String line = "";
      while ((line = br.readLine()) != null) {
        String row = line.trim();
        if (formatNulls) {
          row += ",-NA-,";
          row += line.trim();
        }
        expectedRows.add(row);
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }
    assertEquals(actualRows, expectedRows);
  }

  @Test
  public void testPersistentResultSet() throws Exception {
    createTestTable("test_persistent_result_set");
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    conf.setBoolean(GrillConfConstants.GRILL_ADD_INSERT_OVEWRITE, true);
    conf.set(GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR, TEST_OUTPUT_DIR);
    QueryContext ctx = new QueryContext("SELECT ID FROM test_persistent_result_set", null, conf);
    GrillResultSet resultSet = driver.execute(ctx);
    validatePersistentResult(resultSet, TEST_DATA_FILE, ctx.getHDFSResultDir(), false);
    Assert.assertEquals(0, driver.getHiveHandleSize());

    ctx = new QueryContext("SELECT ID FROM test_persistent_result_set", null, conf);
    driver.executeAsync(ctx);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(ctx, DriverQueryState.SUCCESSFUL, true, false);
    driver.closeQuery(ctx.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    conf.set(GrillConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT,
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'" +
            " WITH SERDEPROPERTIES ('serialization.null.format'='-NA-'," +
        " 'field.delim'=','  ) STORED AS TEXTFILE ");
    ctx = new QueryContext("SELECT ID, null, ID FROM test_persistent_result_set", null, conf);
    resultSet = driver.execute(ctx);
    Assert.assertEquals(0, driver.getHiveHandleSize());
    validatePersistentResult(resultSet, TEST_DATA_FILE, ctx.getHDFSResultDir(), true);
    driver.closeQuery(ctx.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    ctx = new QueryContext("SELECT ID, null, ID FROM test_persistent_result_set", null, conf);
    driver.executeAsync(ctx);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(ctx, DriverQueryState.SUCCESSFUL, true, true);
    driver.closeQuery(ctx.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  private void waitForAsyncQuery(QueryContext ctx, HiveDriver driver) throws Exception {
    while (true) {
      driver.updateStatus(ctx);
      System.out.println("#W Waiting for query " + ctx.getQueryHandle() + " status: " + ctx.getDriverStatus().getState());
      assertNotNull(ctx.getDriverStatus());
      if (ctx.getDriverStatus().isFinished()) {
        assertTrue(ctx.getDriverStatus().getDriverFinishTime() > 0);
        break;
      }
      System.out.println("Progress:" + ctx.getDriverStatus().getProgressMessage());
      Thread.sleep(1000);
      assertTrue(ctx.getDriverStatus().getDriverStartTime() > 0);
    }
  }

  // explain
  @Test
  public void testExplain() throws Exception {
    createTestTable("test_explain");
    DriverQueryPlan plan = driver.explain("SELECT ID FROM test_explain", conf);
    assertTrue(plan instanceof HiveQueryPlan);
    assertEquals(plan.getTableWeight("test_explain"), 500.0);
    Assert.assertEquals(0, driver.getHiveHandleSize());

    // test execute prepare
    PreparedQueryContext pctx = new PreparedQueryContext(
        "SELECT ID FROM test_explain", null, conf);
    plan = driver.explainAndPrepare(pctx);
    QueryContext qctx = new QueryContext(pctx, null, conf);
    GrillResultSet result = driver.execute(qctx);
    Assert.assertEquals(0, driver.getHiveHandleSize());
    validateInMemoryResult(result);

    // test execute prepare async
    qctx = new QueryContext(pctx, null, conf);
    driver.executeAsync(qctx);
    assertNotNull(qctx.getDriverOpHandle());
    validateExecuteAsync(qctx, DriverQueryState.SUCCESSFUL, false, false);
    Assert.assertEquals(1, driver.getHiveHandleSize());

    driver.closeQuery(qctx.getQueryHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    // for backward compatibility
    qctx = new QueryContext(pctx, null, conf);
    qctx.setQueryHandle(new QueryHandle(pctx.getPrepareHandle().getPrepareHandleId()));
    result = driver.execute(qctx);
    assertNotNull(qctx.getDriverOpHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());
    validateInMemoryResult(result);
    // test execute prepare async
    qctx = new QueryContext(pctx, null, conf);
    qctx.setQueryHandle(new QueryHandle(pctx.getPrepareHandle().getPrepareHandleId()));
    driver.executeAsync(qctx);
    Assert.assertEquals(1, driver.getHiveHandleSize());
    validateExecuteAsync(qctx, DriverQueryState.SUCCESSFUL, false, false);

    driver.closeQuery(qctx.getQueryHandle());
    driver.closePreparedQuery(pctx.getPrepareHandle());
    Assert.assertEquals(0, driver.getHiveHandleSize());

    conf.setBoolean(GrillConfConstants.PREPARE_ON_EXPLAIN, false);
    plan = driver.explain("SELECT ID FROM test_explain", conf);
    assertTrue(plan instanceof HiveQueryPlan);
    assertNull(plan.getHandle());
    conf.setBoolean(GrillConfConstants.PREPARE_ON_EXPLAIN, true);
    Assert.assertEquals(0, driver.getHiveHandleSize());
  }

  @Test
  public void testExplainOutput() throws Exception {
    createTestTable("explain_test_1");
    createTestTable("explain_test_2");

    DriverQueryPlan plan = driver.explain("SELECT explain_test_1.ID, count(1) FROM " +
        " explain_test_1  join explain_test_2 on explain_test_1.ID = explain_test_2.ID" +
        " WHERE explain_test_1.ID = 'foo' or explain_test_2.ID = 'bar'" +
        " GROUP BY explain_test_1.ID", conf);

    Assert.assertEquals(0, driver.getHiveHandleSize());
    assertTrue(plan instanceof  HiveQueryPlan);
    assertNotNull(plan.getTablesQueried());
    assertEquals(plan.getTablesQueried().size(), 2);
    assertNotNull(plan.getTableWeights());
    assertTrue(plan.getTableWeights().containsKey("explain_test_1"));
    assertTrue(plan.getTableWeights().containsKey("explain_test_2"));
    assertEquals(plan.getNumJoins(), 1);
    assertTrue(plan.getPlan() != null && !plan.getPlan().isEmpty());
    driver.closeQuery(plan.getHandle());
  }

  @Test
  public void testExplainOutputPersistent() throws Exception {
    createTestTable("explain_test_1");
    conf.setBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, true);
    String query2 = "SELECT DISTINCT ID FROM explain_test_1";
    PreparedQueryContext pctx = new PreparedQueryContext(query2, null, conf);
    DriverQueryPlan plan2 = driver.explainAndPrepare(pctx);
    //assertNotNull(plan2.getResultDestination());
    Assert.assertEquals(0, driver.getHiveHandleSize());
    assertNotNull(plan2.getTablesQueried());
    assertEquals(plan2.getTablesQueried().size(), 1);
    assertTrue(plan2.getTableWeights().containsKey("explain_test_1"));
    assertEquals(plan2.getNumSels(), 1);
    QueryContext ctx = new QueryContext(pctx, null, conf);
    GrillResultSet resultSet = driver.execute(ctx);
    Assert.assertEquals(0, driver.getHiveHandleSize());
    HivePersistentResultSet persistentResultSet = (HivePersistentResultSet) resultSet;
    String path = persistentResultSet.getOutputPath();
    assertEquals(ctx.getHdfsoutPath(), path);
    driver.closeQuery(plan2.getHandle());
  }
}
