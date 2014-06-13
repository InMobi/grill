package com.inmobi.grill.server.ml;

import com.inmobi.grill.api.StringList;
import com.inmobi.grill.api.ml.ModelMetadata;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.api.ml.MLModel;
import com.inmobi.grill.server.ml.spark.TestHiveTableRDD;
import com.inmobi.grill.server.ml.spark.trainers.LogisticRegressionTrainer;
import com.inmobi.grill.server.ml.spark.trainers.NaiveBayesTrainer;
import com.inmobi.grill.server.ml.spark.trainers.SVMTrainer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNotNull;

@Test(groups = "ml")
public class TestMLResource extends GrillJerseyTest {
  public static final Log LOG = LogFactory.getLog(TestHiveTableRDD.class);
  private transient HiveConf conf;
  private transient ThriftCLIServiceClient hiveClient;
  private transient SessionHandle session;
  private transient Map<String, String> confOverlay = new HashMap<String, String>();

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    if (System.getenv("SPARK_HOME") == null) {
      fail("SPARK_HOME is not set");
    }

    conf = new HiveConf(TestModelUDF.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    hiveClient = new ThriftCLIServiceClient(new EmbeddedThriftBinaryCLIService());
    session = hiveClient.openSession("anonymous", "anonymous", confOverlay);
    createTable("ml_resource_test", 3, "ml_test_data/nbayes_data");
    FunctionRegistry.registerGenericUDF(false, HiveMLUDF.UDF_NAME, HiveMLUDF.class);
  }

  @AfterTest
  public void tearDown() throws Exception {
    hiveClient.executeStatement(session, "DROP TABLE IF EXISTS ml_resource_test", confOverlay);
    super.tearDown();
  }

  @Override
  protected int getTestPort() {
    return 9000;
  }

  @Override
  protected Application configure() {
    return new MLApp();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  private void createTable(String tableName, int numFeatures, String dataFile) throws Exception {
    StringBuilder createTableQuery = new StringBuilder("CREATE TABLE "+ tableName +"(label double, ");
    String features[] = new String[numFeatures];
    for (int i = 1; i <= numFeatures; i++) {
      features[i-1] = "feature_" + i + " double";
    }

    createTableQuery.append(StringUtils.join(features, ", "))
      .append(")")
      .append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '");
    LOG.info("@@Creating table " + createTableQuery.toString());
    hiveClient.executeStatement(session, createTableQuery.toString(), confOverlay);
    // Load data into the table

    File dataf = new File(dataFile);
    assertTrue(dataf.exists(), dataf.getAbsolutePath()  + " does not exist");

    BufferedReader br = null;
    PrintWriter out = null;
    String fileName = "target/" + tableName + ".data";
    try {
      br = new BufferedReader(new FileReader(dataFile));
      out = new PrintWriter(fileName);
      String line;

      int records = 0;
      while ((line = br.readLine()) != null) {
        out.println(line.replace(",", " ").trim());
        records++;
      }
      assertTrue(records > 0, "Expecting non empty data file");
      LOG.info("@@Loading " + records  + " in table " + tableName);
    } finally {
      if (out != null) {
        out.flush();
        out.close();
      }
    }

    hiveClient.executeStatement(session,
      "LOAD DATA LOCAL INPATH '" + fileName + "' INTO TABLE " + tableName, confOverlay);
  }

  @Test
  public void testGetTrainers() throws Exception {
    WebTarget target = target("ml").path("trainers");
    StringList trainers = target.request().get(StringList.class);
    assertNotNull(trainers);
    assertEquals(trainers.getElements().size(), 3);
    assertEquals(new HashSet<String>(trainers.getElements()),
      new HashSet<String>(Arrays.asList(NaiveBayesTrainer.NAME,
        SVMTrainer.NAME,
        LogisticRegressionTrainer.NAME)));
  }

  @Test
  public void testTrain() throws Exception {
    final String algo = NaiveBayesTrainer.NAME;
    WebTarget target = target("ml").path(algo).path("train");
    Form params = new Form();
    params.param("table", "ml_resource_test");
    params.param("-label", "label");
    params.param("-feature", "feature_1");
    params.param("-feature", "feature_2");
    params.param("-feature", "feature_3");
    params.param("-lambda", 0.8 + "");

    String modelID = target
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(params, MediaType.APPLICATION_FORM_URLENCODED_TYPE), String.class);
    assertNotNull(modelID);
    System.out.println("@@ model = " + modelID);

    // Check model ID exists
    MLModel model = ModelLoader.loadModel(new JobConf(conf), algo, modelID);
    assertNotNull(model);
    assertEquals(model.getId(), modelID);
    assertEquals(model.getTable(), "ml_resource_test");

    // Test the model using a UDF
    hiveClient.executeStatement(session, "INSERT OVERWRITE LOCAL DIRECTORY 'target/test_rest_call_model' " +
      "SELECT predict('" + algo + "', '"+modelID+"', feature_1, feature_2, feature_3) " +
      "FROM ml_resource_test", confOverlay);

    // Read the file back
    List<String> lines = new ArrayList<String>();
    for (File part : new File("target/test_rest_call_model").listFiles()) {
      lines.addAll(FileUtils.readLines(part));
    }
    assertNotNull(lines);
    assertTrue(lines.size() > 0);
    System.out.println("@@Predictions: " + lines);

    // Test get model list
    StringList models = target("ml").path("models").path(algo).request().get(StringList.class);
    assertNotNull(models.getElements());
    assertTrue(models.getElements().contains(modelID));

    // Get single model
    ModelMetadata meta = target("ml").path("models").path(algo).path(modelID)
      .request().get(ModelMetadata.class);

    assertEquals(meta.getModelID(), modelID);
    assertEquals(meta.getTable(), "ml_resource_test");
    assertEquals(meta.getAlgorithm(), NaiveBayesTrainer.NAME);
    assertEquals(meta.getCreatedAt(), model.getCreatedAt().toString());
  }

  @Test
  public void testClearModelCache() throws Exception {
    WebTarget target = target("ml").path("clearModelCache");
    Response response = target.request().delete();
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
  }
}
