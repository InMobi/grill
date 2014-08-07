package com.inmobi.grill.server.query;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.*;
import com.inmobi.grill.driver.hive.HiveDriver;
import com.inmobi.grill.driver.jdbc.JDBCDriver;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.metrics.MetricsService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.testng.Assert;
import org.testng.annotations.*;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import static org.testng.Assert.*;


@Test(groups = "integration")
public class TestJDBCDriverIntegration extends TestQueryService {
  public static final Log LOG = LogFactory.getLog(TestJDBCDriverIntegration.class);
  private HiveConf conf = new HiveConf();
  private JDBCDriver driver;
  private HiveDriver hiveDriver;
  final int NUM_RECORDS = 10;

  @BeforeClass
  @Override
  public void createTables() throws InterruptedException {
    try {
      super.setUp();
    } catch (Exception e) {
      e.printStackTrace();
    }
    driver = new JDBCDriver();
    queryService = (QueryExecutionServiceImpl) GrillServices.get().getService("query");
    metricsSvc = (MetricsService)GrillServices.get().getService(MetricsService.NAME);
    try {
      driver.configure(conf);
      grillSessionId = queryService.openSession("foo", "bar", new HashMap<String, String>());
    } catch (GrillException e) {
      e.printStackTrace();
    }
    createTable(testTable, target(), grillSessionId);
    loadData(testTable, TEST_DATA_FILE);
  }

  @Override
  protected int getTestPort() {
    return 9988;
  }

  @BeforeMethod
  public void addJDBCDriver() throws Exception{
    if (!queryService.drivers.containsKey(JDBCDriver.class.getName())) {
      queryService.drivers.put(JDBCDriver.class.getName(), driver);
      hiveDriver = (HiveDriver) queryService.drivers.remove(HiveDriver.class.getName());
    }
  }

  @AfterMethod
  public void resetDrivers() throws Exception {
    queryService.drivers.remove(JDBCDriver.class.getName());
    queryService.drivers.put(HiveDriver.class.getName(), hiveDriver);
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
        if (i % 2 == 0) {
          stmt.setString(2, "" + i);
        } else {
          stmt.setString(2, null);
        }
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
      if (i % 2 == 0) {
        assertEquals(values.get(1), "" + i);
      } else {
        assertNull(values.get(1));
      }
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
    } else if (resultset instanceof PersistentQueryResult) {
      LOG.info(">> Validating persistent query result "
        + ((PersistentQueryResult) resultset).getPersistedURI());
      List<String> rows = readResultSet((PersistentQueryResult) resultset, handle, isDir);
      assertEquals(rows.size(), NUM_RECORDS);
    }
  }

  @Override
  void validateResultSetMetadata(QueryHandle handle, String outputTablePfx, WebTarget parent, GrillSessionHandle grillSessionId) {
    final WebTarget target = parent.path("queryapi/queries");

    QueryResultSetMetadata metadata = target.path(handle.toString()).path(
      "resultsetmetadata").queryParam("sessionid", grillSessionId).request().get(QueryResultSetMetadata.class);
    Assert.assertEquals(metadata.getColumns().size(), 2);
    assertTrue(metadata.getColumns().get(0).getName().toLowerCase().equals((outputTablePfx + "ID").toLowerCase()) ||
      metadata.getColumns().get(0).getName().toLowerCase().equals("ID".toLowerCase()));
    assertEquals(metadata.getColumns().get(0).getType().name().toLowerCase(), "INT".toLowerCase());
    assertTrue(metadata.getColumns().get(1).getName().toLowerCase().equals((outputTablePfx + "IDSTR").toLowerCase()) ||
      metadata.getColumns().get(0).getName().toLowerCase().equals("IDSTR".toLowerCase()));
    assertEquals(metadata.getColumns().get(1).getType().name().toLowerCase(), "VARCHAR".toLowerCase());
  }

  @Test
  public void testJDBCPersistentResultSet() throws Exception {
    final WebTarget target = target().path("queryapi/queries");
    final GrillConf conf = new GrillConf();
    final FormDataMultiPart mp = new FormDataMultiPart();
    conf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, "true");
    conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    //conf.addProperty(GrillConfConstants.QUERY_OUTPUT_SERDE, LazySimpleSerDe.class.getCanonicalName());
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
      grillSessionId, MediaType.APPLICATION_XML_TYPE));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
      "select ID from " + testTable));
    mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
      "operation").build(),
      "execute"));
    mp.bodyPart(new FormDataBodyPart(
      FormDataContentDisposition.name("conf").fileName("conf").build(),
      conf,
      MediaType.APPLICATION_XML_TYPE));
    QueryHandle handle = target.request().post(
      Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

    Assert.assertNotNull(handle);

    // Get query
    GrillQuery ctx = target.path(handle.toString()).queryParam("sessionid",
      grillSessionId).request().get(GrillQuery.class);
    // wait till the query finishes
    QueryStatus stat = ctx.getStatus();
    while (!stat.isFinished()) {
      ctx = target.path(handle.toString()).queryParam("sessionid",
        grillSessionId).request().get(GrillQuery.class);
      stat = ctx.getStatus();
      Thread.sleep(1000);
    }

    if (stat.getStatus().equals(QueryStatus.Status.SUCCESSFUL)) {
      // fetch results
      QueryResult resultset = target.path(handle.toString()).path(
        "resultset").queryParam("sessionid", grillSessionId).request().get(QueryResult.class);
      validatePersistentResult(resultset, handle, false);
    } else {
      fail("Query failed ");
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
