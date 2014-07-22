package com.inmobi.grill.server.query;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.InMemoryQueryResult;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryResult;
import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.driver.hive.HiveDriver;
import com.inmobi.grill.driver.jdbc.JDBCDriver;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.metrics.MetricsService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.testng.annotations.*;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


@Test(groups = "integration", suiteName = "queryServiceTestsWithJDBCDriver")
public class TestJDBCDriverIntegration{}/* extends TestQueryService {
  public static final Log LOG = LogFactory.getLog(TestJDBCDriverIntegration.class);
  private HiveConf conf;
  private JDBCDriver driver;
  final int NUM_RECORDS = 10;

  @Override
  protected int getTestPort() {
    return 8084;
  }

  @BeforeSuite
  @Override
  public void startAll() throws Exception {
    conf = new HiveConf();
    conf.set(GrillConfConstants.ENGINE_DRIVER_CLASSES, JDBCDriver.class.getName());
    GrillServices.get().init(conf);
    GrillServices.get().start();
    LOG.info("@@@@ STARTED SERVICES");
  }

  @AfterSuite
  @Override
  public void stopAll() throws Exception {
    GrillServices.get().stop();
  }

  @BeforeTest
  public void setUpServicesJDBCDriver() throws Exception {
    queryService = (QueryExecutionServiceImpl) GrillServices.get().getService("query");
    // Remove the Hive driver for this test
    queryService.drivers.remove(HiveDriver.class.getName());
    metricsSvc = (MetricsService)GrillServices.get().getService(MetricsService.NAME);
    grillSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
    driver = (JDBCDriver) queryService.drivers.get(JDBCDriver.class.getName());
  }

  @AfterTest
  public void tearDownServicesJDBCDriver() throws Exception {
    try {
      queryService.closeSession(grillSessionId);
    } catch (Exception e) {
      e.printStackTrace();
    }

    super.tearDown();
  }

  @Override
  public void tearDownServices() throws Exception {
    // Remove implementation so that tear down doesn't get called twice
  }

  @Override
  void loadData(String tblName,
                String TEST_DATA_FILE,
                WebTarget parent,
                GrillSessionHandle grillSessionId) throws InterruptedException {
    Connection conn = null;
    PreparedStatement stmt = null;
    try {
      conn = driver.getConnection(conf);
      stmt = conn.prepareStatement("INSERT INTO " + tblName + " VALUES(?, ?)");
      for (int i = 0; i < NUM_RECORDS; i++) {
        stmt.setInt(1, i);
        stmt.setString(2, "" + i);
        stmt.executeUpdate();
      }
      conn.commit();
      LOG.info("@@ Inserted " + NUM_RECORDS + " in " + tblName);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }


  @Override
  void createTable(String tblName, WebTarget parent, GrillSessionHandle grillSessionId) throws InterruptedException {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = driver.getConnection(conf);
      stmt = conn.createStatement();
      stmt.execute("CREATE TABLE " + tblName + " (ID INT, IDSTR VARCHAR(100))");
      conn.commit();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @AfterClass
  @Override
  public void dropTables() throws InterruptedException {
    // not required since hsql is started in memory only mode
  }

  @Override
  void validateInmemoryResult(InMemoryQueryResult resultset) {
    int i = 0;
    assertNotNull(resultset.getRows());
    assertEquals(resultset.getRows().size(), NUM_RECORDS);
    for (ResultRow row : resultset.getRows()) {
      List<Object> values = row.getValues();
      assertNotNull(values);
      assertEquals(values.size(), 2);
      assertEquals(values.get(0), i);
      assertEquals(values.get(1), "" + i);
      i++;
    }
  }

  @Override
  void validatePersistedResult(QueryHandle handle,
                               WebTarget parent,
                               GrillSessionHandle grillSessionId,
                               boolean isDir) throws IOException {
    final WebTarget target = parent.path("queryapi/queries");
    // fetch results
    validateResultSetMetadata(handle, parent, grillSessionId);

    QueryResult resultset = target.path(handle.toString()).path(
      "resultset").queryParam("sessionid", grillSessionId).request().get(QueryResult.class);
    validatePersistentResult(resultset, handle, isDir);

    if (isDir) {
      validNotFoundForHttpResult(parent, grillSessionId, handle);
    }
  }

  @Override
  void validatePersistentResult(QueryResult resultset, QueryHandle handle, boolean isDir) throws IOException {
    if (resultset instanceof InMemoryQueryResult) {
      validateInmemoryResult((InMemoryQueryResult) resultset);
    }
  }

  @Override
  public void testGrillServerRestart() throws InterruptedException, IOException, GrillException {
    // Not required - if Grill server goes down while DB is up, there is no way to recover lost query
  }

  @Override
  public void testHiveServerRestart() throws Exception {
    // Not required
  }

  @Override
  public void testExecuteAsyncTempTable() throws InterruptedException, IOException {
    // Not required as DDL statement not supported by JDBC driver
  }

  @Override
  public void testExplainAndPrepareQuery() throws InterruptedException {
    // Not required since explain is not supported by JDBC driver
  }

  @Override
  public void testExplainQuery() throws InterruptedException {
    // Not required since explain is not supported by JDBC driver
  }


  @Override
  public void testLaunchFail() throws InterruptedException {
    // Need to change implementation to validate in case of JDBC driver.
  }
}

**/