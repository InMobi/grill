package com.inmobi.grill.server.ml;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.Function1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

@Test(groups = "ml")
public class TestHiveTableRDD implements Serializable {
  public static final Log LOG = LogFactory.getLog(TestHiveTableRDD.class);

  private transient ThriftCLIServiceClient hiveClient;
  private transient SessionHandle session;
  private transient Map<String, String> confOverlay = new HashMap<String, String>();
  public static final String DATA_FILE = "ml_test_data/lr.data";

  @BeforeClass
  public void setup() throws Exception {
    if (System.getenv("SPARK_HOME") == null) {
      fail("SPARK_HOME is not set");
    }

    HiveConf conf = new HiveConf(TestModelUDF.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    hiveClient = new ThriftCLIServiceClient(new EmbeddedThriftBinaryCLIService());
    session = hiveClient.openSession("anonymous", "anonymous", confOverlay);
    createTable();
  }

  @AfterClass
  public void destroy() throws Exception {
    hiveClient.executeStatement(session, "DROP TABLE IF EXISTS rdd_test_table", confOverlay);
  }


  private void createTable() throws Exception {
    hiveClient.executeStatement(session,
      "CREATE TABLE rdd_test_table(label double, feature_1 double, feature_2 double)" +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '",
      confOverlay);

    // Load data into the table
    BufferedReader br = new BufferedReader(new FileReader(DATA_FILE));
    PrintWriter out = new PrintWriter("target/rdd_test_table.data");
    String line;

    while ((line = br.readLine()) != null) {
      out.println(line.replace(",", " ").trim());
    }

    hiveClient.executeStatement(session,
      "LOAD DATA LOCAL INPATH 'target/rdd_test_table.data' INTO TABLE rdd_test_table", confOverlay);
  }

  @Test
  public void testHiveTableRDD() throws Exception {
    LOG.info("@@ Start hive table rdd test");
    SparkConf sparkConf = new SparkConf()
      .setAppName("SparkTest")
        // Expect that SPARK_HOME is set for the test
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setMaster("local");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    final double trainingFaction = 0.7;
    // Spec with all details
    TableTrainingSpec trainingSpec = TableTrainingSpec.newBuilder()
      .hiveConf(new HiveConf())
      .database("default")
      .table("rdd_test_table")
      .partitionFilter(null)
      .labelColumn("label")
      .featureColumns(Arrays.asList("feature_1", "feature_2"))
      .trainingFraction(trainingFaction)
      .build();

    boolean isValid = trainingSpec.validate();
    assertTrue(isValid, "Test table spec is valid");
    assertEquals(trainingSpec.labelPos, 0);
    assertEquals(trainingSpec.featurePositions, new int[]{1, 2});
    assertEquals(trainingSpec.numFeatures, 2);

    // Spec with min required details
    TableTrainingSpec trainingSpec2 = TableTrainingSpec.newBuilder()
      .hiveConf(new HiveConf())
      .table("rdd_test_table")
      .labelColumn("label")
      .build();

    assertTrue(trainingSpec2.validate());
    assertEquals(trainingSpec2.labelPos, 0);
    assertEquals(trainingSpec2.featurePositions, new int[]{1, 2});
    assertEquals(trainingSpec2.numFeatures, 2);

    LOG.info("@@ Training spec validated");

    try {
      trainingSpec.createRDDs(sc);
    } catch (Exception exc) {
      exc.printStackTrace();
      throw exc;
    }

    LOG.info("@@ RDDs created");

    long totalSamples = trainingSpec.labeledRDD.count();
    LOG.info("@@ Total size " + totalSamples);


    RDD<LabeledPoint> trainingRDD = trainingSpec.getTrainingRDD();
    long trainingSamples = trainingRDD.toJavaRDD().count();
    LOG.info("@@ Training size "  + trainingSamples);

    RDD<LabeledPoint> testingRDD = trainingSpec.getTestingRDD();
    long testingSamples = testingRDD.toJavaRDD().count();
    LOG.info("@@ Testing size "  + testingSamples);

    // Train a model using the RDD
    LOG.info("@@ Training model");
    final LogisticRegressionModel model = LogisticRegressionWithSGD.train(trainingSpec.labeledRDD.rdd(), 10, 0.1);
    assertNotNull(model);

    LOG.info("@@ Running test on testing RDD");
    // Just verify if model is able to predict
    JavaRDD<Double> prediction = testingRDD.toJavaRDD().map(new Function<LabeledPoint, Double>() {
      @Override
      public Double call(LabeledPoint v1) throws Exception {
        return model.predict(v1.features());
      }
    });

    List<Double> predictedOutput = prediction.collect();
    assertNotNull(predictedOutput);
    assertTrue(!predictedOutput.isEmpty());
    sc.stop();
    LOG.info("@@ End hive table rdd test");
  }
}
