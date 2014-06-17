package com.inmobi.grill.server.ml.spark;

import com.inmobi.grill.server.api.ml.MLModel;
import com.inmobi.grill.server.api.ml.MLTrainer;
import com.inmobi.grill.server.ml.TestModelUDF;
import com.inmobi.grill.server.ml.spark.trainers.LogisticRegressionTrainer;
import com.inmobi.grill.server.ml.spark.trainers.NaiveBayesTrainer;
import com.inmobi.grill.server.ml.spark.trainers.SVMTrainer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "ml")
public class TestSparkTrainers {
  public static final Log LOG = LogFactory.getLog(TestHiveTableRDD.class);

  private transient ThriftCLIServiceClient hiveClient;
  private transient SessionHandle session;
  private transient Map<String, String> confOverlay = new HashMap<String, String>();
  public JavaSparkContext sparkCtx;
  private transient HiveConf conf = new HiveConf();
  private SparkMLDriver sparkMLDriver;

  @BeforeClass
  public void setup() throws Exception {
    if (System.getenv("SPARK_HOME") == null) {
      fail("SPARK_HOME is not set");
    }

    HiveConf conf = new HiveConf(TestModelUDF.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    hiveClient = new ThriftCLIServiceClient(new EmbeddedThriftBinaryCLIService());
    session = hiveClient.openSession("anonymous", "anonymous", confOverlay);
    SparkConf sparkConf = new SparkConf()
      .setAppName("SparkTest")
        // Expect that SPARK_HOME is set for the test
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setMaster("local");

    sparkCtx = new JavaSparkContext(sparkConf);
    sparkMLDriver = new SparkMLDriver();
    sparkMLDriver.init(conf);
    sparkMLDriver.start();
  }

  @AfterClass
  public void destroy() throws Exception {
    sparkMLDriver.stop();
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

  private String getFeatureLabelArg(int numFeatures) {
    StringBuilder builder = new StringBuilder("label label ");
    for (int i = 1; i <= numFeatures; i++) {
      builder.append("feature feature_").append(i).append(" ");
    }
    return builder.toString();
  }

  private void testPredict(MLModel model, int numFeatures) {
    double features[] = new double[numFeatures];
    for (int i = 0; i < numFeatures; i++) {
      features[i] = 1.0 + Math.random();
    }
    double classLabel = model.predict(features);
  }

  private void testTrainer(String trainerName, String dataFile, int numFeatures,
                           MLTrainer trainer, String trainerParams) throws Exception {
    String tableName = trainerName + "_table";
    try {
      createTable(trainerName + "_table", numFeatures, dataFile);
      LOG.info("@@Begin training " + trainerName + " trainer params: " + trainerParams);
      MLModel model =
        trainer.train(conf, "default", tableName, trainerName + "0.1", StringUtils.split(trainerParams, " "));
      assertNotNull(model);
      //testPredict(model, numFeatures);
      LOG.info("@@Done trainer " + trainerName);
    } finally {
      hiveClient.executeStatement(session, "DROP TABLE IF EXISTS " + tableName, confOverlay);
    }
  }

  @Test
  public void testLRTrainer() throws Exception {
    LogisticRegressionTrainer lrTrainer = (LogisticRegressionTrainer)
      sparkMLDriver.getTrainerInstance(LogisticRegressionTrainer.NAME);

    testTrainer("lr", "ml_test_data/lr.data", 2, lrTrainer, getFeatureLabelArg(2) +
      " iterations 10 stepSize 1.0 minBatchFraction 1.0");
  }

  @Test
  public void testNaiveBayesTrainer() throws Exception {
    NaiveBayesTrainer naiveBayesTrainer = (NaiveBayesTrainer)
      sparkMLDriver.getTrainerInstance(NaiveBayesTrainer.NAME);
    testTrainer("naive_bayes", "ml_test_data/nbayes_data", 3, naiveBayesTrainer, getFeatureLabelArg(3));
  }

  @Test
  public void testSVMTrainer() throws Exception {
    SVMTrainer svmTrainer = (SVMTrainer) sparkMLDriver.getTrainerInstance(SVMTrainer.NAME);
    testTrainer("svm", "ml_test_data/svm_data", 16, svmTrainer,
      getFeatureLabelArg(16) + " iterations 10 stepSize 1.0 minBatchFraction 1.0");
  }
}
