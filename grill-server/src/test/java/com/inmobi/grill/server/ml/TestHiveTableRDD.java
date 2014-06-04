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
      "CREATE TABLE rdd_test_table(feature_1 double, feature_2 double)" +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '",
      confOverlay);

    // Load data into the table
    BufferedReader br = new BufferedReader(new FileReader(DATA_FILE));
    PrintWriter out = new PrintWriter("target/rdd_test_table.data");
    String line;

    while ((line = br.readLine()) != null) {
      out.println(line.substring(line.indexOf(',') + 1).trim());
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
    rdd.cache();
    long count = rdd.count();
    LOG.info("@@ Records in RDD = " + count);

    JavaRDD<Double> feature1Rdd = rdd.map(new TestTableProcessor()).cache();
    LOG.info("@@ Created double RDD");


    List<Double> feature1Vals = feature1Rdd.collect();
    int actualRecordCount = 0;
    for (Double d : feature1Vals) {
      actualRecordCount++;
    }

    LOG.info("@@ Actual Records = " + actualRecordCount);
    assertEquals(actualRecordCount, count);
    sc.stop();
    LOG.info("@@ End hive table rdd test");
  }

  public static class TestTableProcessor implements Function<Tuple2<WritableComparable, HCatRecord>, Double>, Serializable {
    @Override
    public Double call(Tuple2<WritableComparable, HCatRecord> t) throws Exception {
      return (Double) t._2().get(0);
    }
  }
}
