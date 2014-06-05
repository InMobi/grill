package com.inmobi.grill.server.ml;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;
@Test(groups = "ml")
public class TestHiveTableRDD {
  public static final Log LOG = LogFactory.getLog(TestHiveTableRDD.class);

  private ThriftCLIServiceClient hiveClient;
  private SessionHandle session;
  private Map<String, String> confOverlay = new HashMap<String, String>();
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
    JavaPairRDD<WritableComparable, HCatRecord> rdd =
      HiveTableRDD.createHiveTableRDD(sc, new Configuration(), null, "rdd_test_table", null);
    assertNotNull(rdd);

    // Do an action on the RDD
    LOG.info("@@ Starting training on table RDD");
    // Create trainable RDD from the table
    final int labelPos = 0;
    final int featurePositions[] = {1, 2};
    final DoubleValueMapper doubleValueMapper = new DoubleValueMapper();
    final FeatureValueMapper[] valueMappers = {doubleValueMapper, doubleValueMapper};

    ColumnFeatureFunction featureMapper = new ColumnFeatureFunction(featurePositions,
      valueMappers,
      labelPos,
      featurePositions.length, 0);

    JavaRDD<LabeledPoint> trainableRDD = rdd.map(featureMapper);

    // Train a model using the RDD
    LogisticRegressionModel model = LogisticRegressionWithSGD.train(trainableRDD.rdd(),
      10, 0.1);
    assertNotNull(model);

    // Just verify if model is able to predict
    double testVector[] = {1.0, 1.0};
    double prediction = model.predict(Vectors.dense(testVector));
    LOG.info("@@ Prediction for test vector: " + prediction);
    sc.stop();
    LOG.info("@@ End hive table rdd test");
  }

  public static class TestTableProcessor implements Function<Tuple2<WritableComparable, HCatRecord>, Double>, Serializable {
    @Override
    public Double call(Tuple2<WritableComparable, HCatRecord> t) throws Exception {
      HCatRecord rec = t._2();
      System.out.println("@@ " + rec.get(0) + " | " + rec.get(1)  + " | " + rec.get(2));
      return (Double) rec.get(0);
    }
  }
}
