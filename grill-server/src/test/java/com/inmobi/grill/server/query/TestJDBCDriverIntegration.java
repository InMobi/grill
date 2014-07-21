package com.inmobi.grill.server.query;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.*;
import com.inmobi.grill.driver.hive.HiveDriver;
import com.inmobi.grill.driver.jdbc.JDBCDriver;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.metrics.MetricsService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.testng.Assert;
import org.testng.annotations.*;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Test(groups = "integration", suiteName = "queryServiceTestsWithJDBCDriver")
public class TestJDBCDriverIntegration extends TestQueryService {
  public static final Log LOG = LogFactory.getLog(TestJDBCDriverIntegration.class);
  private HiveConf conf;
  private JDBCDriver driver;

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
    //
  }

  @Override
  protected Application configure() {
    return super.configure();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    super.configureClient(config);
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

      for (int i = 0; i < 10; i++) {
        stmt.setInt(1, i);
        stmt.setString(2, "" + i);
        stmt.executeUpdate();
      }
      conn.commit();
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
        e.printStackTrace();;
      }
    }
  }

  @AfterClass
  @Override
  public void dropTables() throws InterruptedException {
    // not required
  }

  @Override
  void validateHttpEndPoint(WebTarget parent, GrillSessionHandle grillSessionId, QueryHandle handle, String redirectUrl) throws IOException {
  }

  @Override
  void validateInmemoryResult(InMemoryQueryResult resultset) {
  }

  @Override
  void validatePersistedResult(QueryHandle handle, WebTarget parent, GrillSessionHandle grillSessionId, boolean isDir) throws IOException {
  }

  @Override
  void validatePersistentResult(QueryResult resultset, QueryHandle handle, boolean isDir) throws IOException {
  }

  @Override
  void validateResultSetMetadata(QueryHandle handle, String outputTablePfx, WebTarget parent, GrillSessionHandle grillSessionId) {
  }

  @Override
  void validateResultSetMetadata(QueryHandle handle, WebTarget parent, GrillSessionHandle grillSessionId) {
  }

  @Override
  void validNotFoundForHttpResult(WebTarget parent, GrillSessionHandle grillSessionId, QueryHandle handle) {
  }

  @Override
  public void testGrillServerRestart() throws InterruptedException, IOException, GrillException {
    //
  }

  @Override
  public void testHiveServerRestart() throws Exception {
    //
  }

  @Override
  public void testExecuteAsyncTempTable() throws InterruptedException, IOException {
    // Not required?
  }

  @Override
  public void testExplainAndPrepareQuery() throws InterruptedException {
    //
  }

  @Override
  public void testExplainQuery() throws InterruptedException {
    //
  }


  @Override
  public void testLaunchFail() throws InterruptedException {
    //
  }
}
