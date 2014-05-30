package com.inmobi.grill.server.ml;

import com.inmobi.grill.server.ml.models.SparkLRModel;
import lombok.val;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.testng.annotations.*;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(groups = "unit")
public class TestModelUDF {
  public static final String DATA_FILE = "ml_test_data/lr.data";
  private Path MODEL_LOCATION;
  private CLIServiceClient hiveClient;
  private SessionHandle session;
  Map<String, String> overlay = new HashMap<String, String>();
  private int expectedRows;

  static class ParsePoint implements Function<String, LabeledPoint> {
    private static final Pattern COMMA = Pattern.compile(",");
    private static final Pattern SPACE = Pattern.compile(" ");

    @Override
    public LabeledPoint call(String line) {
      String[] parts = COMMA.split(line);
      double y = Double.parseDouble(parts[0]);
      String[] tok = SPACE.split(parts[1]);
      double[] x = new double[tok.length];
      for (int i = 0; i < tok.length; ++i) {
        x[i] = Double.parseDouble(tok[i]);
      }
      return new LabeledPoint(y, Vectors.dense(x));
    }
  }

  @BeforeClass
  public void setup() throws Exception {
    File modelFile = new File("target/lr_model.out");
    MODEL_LOCATION = new Path(modelFile.toURI());

    HiveConf conf = new HiveConf(TestModelUDF.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    hiveClient = new ThriftCLIServiceClient(new EmbeddedThriftBinaryCLIService());
    session = hiveClient.openSession("anonymous", "anonymous", new HashMap<String, String>());

    createModel();
    createTable();

    FunctionRegistry.registerTemporaryFunction("ml_apply_model",HiveMLUDF.class);
  }

  private void createTable() throws Exception {
    hiveClient.executeStatement(session,
      "CREATE TABLE lr_test_table(feature_1 double, feature_2 double)" +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '",
      overlay);

    // Load data into the table
    BufferedReader br = new BufferedReader(new FileReader(DATA_FILE));
    PrintWriter out = new PrintWriter("target/lr_test_table.data");
    String line;

    while ((line = br.readLine()) != null) {
      out.println(line.substring(line.indexOf(',') + 1).trim());
      expectedRows++;
    }

    hiveClient.executeStatement(session,
      "LOAD DATA LOCAL INPATH 'target/lr_test_table.data' INTO TABLE lr_test_table", overlay);
  }

  @AfterClass
  public void destroy() throws Exception {
    hiveClient.executeStatement(session, "DROP TABLE IF EXISTS lr_test_table", overlay);
  }

  public void createModel() throws Exception {
    if (new File(MODEL_LOCATION.toUri()).exists()) {
      return;
    }

    if (System.getenv("SPARK_HOME") == null) {
      fail("SPARK_HOME is not set");
    }

    SparkConf sparkConf = new SparkConf()
      .setAppName("SparkTest")
      // Expect that SPARK_HOME is set for the test
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setMaster("local");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = sc.textFile(DATA_FILE);
    JavaRDD<LabeledPoint> points = lines.map(new ParsePoint()).cache();
    double stepSize = 0.1;
    int iterations = 100;

    LogisticRegressionModel model = LogisticRegressionWithSGD.train(points.rdd(),
      iterations, stepSize);

    SparkLRModel udfModel = new SparkLRModel("spark-lr-v-1.0", model);
    // Save model to disk

    FileSystem fs = MODEL_LOCATION.getFileSystem(new Configuration());
    fs.deleteOnExit(MODEL_LOCATION);

    ObjectOutputStream out = null;

    try {
      out = new ObjectOutputStream(fs.create(MODEL_LOCATION, true));
      out.writeObject(udfModel);
      out.flush();
    } finally {
      IOUtils.closeQuietly(out);
    }
    sc.stop();
  }

  @Test
  public void testModelUDF() throws Exception {
    Map<String, String> overlay = new HashMap<String, String>();

    // Execute query
    OperationHandle op = hiveClient.executeStatement(session,
      "INSERT OVERWRITE LOCAL DIRECTORY 'target/lr_query_out'" +
        "SELECT ml_apply_model('"+ MODEL_LOCATION.toUri() + "', feature_1, feature_2) FROM lr_test_table", overlay);
    List<String> lines = FileUtils.readLines(new File("target/lr_query_out/000000_0"));
    assertEquals(lines.size(), 647);
  }
}
