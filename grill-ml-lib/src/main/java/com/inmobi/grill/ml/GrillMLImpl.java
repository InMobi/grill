package com.inmobi.grill.ml;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.ml.spark.SparkMLDriver;
import com.inmobi.grill.ml.spark.trainers.BaseSparkTrainer;
import com.inmobi.grill.server.api.GrillConfConstants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.spark.api.java.JavaSparkContext;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

public class GrillMLImpl implements GrillML {
  public static final Log LOG = LogFactory.getLog(GrillMLImpl.class);
  protected List<MLDriver> drivers;
  private HiveConf conf;
  private JavaSparkContext sparkContext;

  public GrillMLImpl(HiveConf conf) {
    this.conf = conf;
  }

  public HiveConf getConf() {
    return conf;
  }

  /**
   * Use an existing Spark context. Useful in case of
   * @param jsc JavaSparkContext instance
   */
  public void setSparkContext(JavaSparkContext jsc) {
    this.sparkContext = jsc;
  }

  public List<String> getAlgorithms() {
    List<String> trainers = new ArrayList<String>();
    for (MLDriver driver : drivers) {
      trainers.addAll(driver.getTrainerNames());
    }
    return trainers;
  }

  public MLTrainer getTrainerForName(String algorithm) throws GrillException {
    for (MLDriver driver : drivers) {
      if (driver.isTrainerSupported(algorithm)) {
        return driver.getTrainerInstance(algorithm);
      }
    }
    throw new GrillException("Trainer not supported " + algorithm);
  }

  public String train(String table, String algorithm, String[] args) throws GrillException {
    MLTrainer trainer = getTrainerForName(algorithm);

    String modelId = UUID.randomUUID().toString();

    LOG.info("Begin training model " + modelId + ", trainer=" + algorithm + ", table=" + table + ", params="
      + Arrays.toString(args));

    String database = null;
    if (SessionState.get() != null) {
      database = SessionState.get().getCurrentDatabase();
    } else {
      database = "default";
    }

    MLModel model = trainer.train(toGrillConf(conf),
      database, table, modelId, args);

    LOG.info("Done training model: " + modelId);

    model.setCreatedAt(new Date());
    model.setTrainerName(algorithm);

    Path modelLocation = null;
    try {
      modelLocation = persistModel(model);
      LOG.info("Model saved: " + modelId + ", trainer: " + algorithm + ", path: " + modelLocation);
      return model.getId();
    } catch (IOException e) {
      throw new GrillException("Error saving model " + modelId + " for trainer " + algorithm, e);
    }
  }

  private Path getTrainerDir(String trainerName) throws IOException{
    String modelSaveBaseDir =
      conf.get(ModelLoader.MODEL_PATH_BASE_DIR, ModelLoader.MODEL_PATH_BASE_DIR_DEFAULT);
    return new Path(new Path(modelSaveBaseDir), trainerName);
  }

  private Path persistModel(MLModel model) throws IOException {
    // Get model save path
    Path trainerDir = getTrainerDir(model.getTrainerName());
    FileSystem fs = trainerDir.getFileSystem(conf);

    if (!fs.exists(trainerDir)) {
      fs.mkdirs(trainerDir);
    }

    Path modelSavePath = new Path(trainerDir, model.getId());
    ObjectOutputStream outputStream = null;

    try {
      outputStream = new ObjectOutputStream(fs.create(modelSavePath, false));
      outputStream.writeObject(model);
      outputStream.flush();
    } catch (IOException io) {
      LOG.error("Error saving model " + model.getId() + " reason: " + io.getMessage());
      throw io;
    } finally {
      IOUtils.closeQuietly(outputStream);
    }
    return modelSavePath;
  }


  public List<String> getModels(String algorithm) throws GrillException {
    try {
      Path trainerDir = getTrainerDir(algorithm);
      FileSystem fs = trainerDir.getFileSystem(conf);
      if (!fs.exists(trainerDir)) {
        return null;
      }

      List<String> models = new ArrayList<String>();

      for (FileStatus stat : fs.listStatus(trainerDir)) {
        models.add(stat.getPath().getName());
      }

      if (models.isEmpty()) {
        return null;
      }

      return models;
    } catch (IOException ioex) {
      throw new GrillException(ioex);
    }
  }

  public MLModel getModel(String algorithm, String modelId) throws GrillException {
    try {
      return ModelLoader.loadModel(conf, algorithm, modelId);
    } catch (IOException e) {
      throw new GrillException(e);
    }
  }

  public synchronized void init(HiveConf hiveConf) {
    this.conf = hiveConf;

    // Get all the drivers
    String[] driverClasses = hiveConf.getStrings("grill.ml.drivers");

    if (driverClasses == null || driverClasses.length == 0) {
      throw new RuntimeException("No ML Drivers specified in conf");
    }

    LOG.info("Loading drivers " + Arrays.toString(driverClasses));
    drivers = new ArrayList<MLDriver>(driverClasses.length);

    for (String driverClass : driverClasses) {
      Class<?> cls;
      try {
        cls = Class.forName(driverClass);
      } catch (ClassNotFoundException e) {
        LOG.error("Driver class not found " + driverClass);
        continue;
      }

      if (!MLDriver.class.isAssignableFrom(cls)) {
        LOG.warn("Not a driver class " + driverClass);
        continue;
      }

      try {
        Class<? extends MLDriver> mlDriverClass = (Class<? extends MLDriver>) cls;
        MLDriver driver = mlDriverClass.newInstance();
        driver.init(toGrillConf(conf));
        drivers.add(driver);
        LOG.info("Added driver " + driverClass);
      } catch (Exception e) {
        LOG.error("Failed to create driver " + driverClass + " reason: " + e.getMessage() , e);
      }
    }
    if (drivers.isEmpty()) {
      throw new RuntimeException("No ML drivers loaded");
    }

    LOG.info("Inited ML service");
  }

  public synchronized void start() {
    for (MLDriver driver : drivers) {
      try {
        if (driver instanceof SparkMLDriver && sparkContext != null) {
          ((SparkMLDriver) driver).useSparkContext(sparkContext);
        }
        driver.start();
      } catch (GrillException e) {
        LOG.error("Failed to start driver " + driver, e);
      }
    }
    LOG.info("Started ML service");
  }

  public synchronized void stop() {
    for (MLDriver driver : drivers) {
      try {
        driver.stop();
      } catch (GrillException e) {
        LOG.error("Failed to stop driver " + driver, e);
      }
    }
    drivers.clear();
    LOG.info("Stopped ML service");
  }

  public synchronized HiveConf getHiveConf() {
    return conf;
  }

  public void clearModels() {
    ModelLoader.clearCache();
  }

  public String getModelPath(String algorithm, String modelID) {
    return ModelLoader.getModelLocation(conf, algorithm, modelID).toString();
  }

  @Override
  public MLTestReport testModel(GrillSessionHandle session, String table, String algorithm, String modelID) throws GrillException {
    return null;
  }

  /**
   * Test a model in embedded mode
   */
  public MLTestReport testModelRemote(GrillSessionHandle sessionHandle,
                                String table,
                                String algorithm,
                                String modelID,
                                String queryApiUrl) throws GrillException {
    return testModel(sessionHandle, table, algorithm, modelID,
      new RemoteQueryRunner(sessionHandle, queryApiUrl));
  }

  public MLTestReport testModel(GrillSessionHandle sessionHandle,
                                String table,
                                String algorithm,
                                String modelID,
                                TestQueryRunner queryRunner) throws GrillException {
    // check if algorithm exists
    if (!getAlgorithms().contains(algorithm)) {
      throw new GrillException("No such algorithm " + algorithm);
    }

    MLModel model;
    try {
      model = ModelLoader.loadModel(conf, algorithm, modelID);
    } catch (IOException e) {
      throw new GrillException(e);
    }

    if (model == null) {
      throw new GrillException("Model not found: " + modelID + " algorithm=" + algorithm);
    }

    String database = null;

    if (SessionState.get() != null) {
      database = SessionState.get().getCurrentDatabase();
    }

    String testID = UUID.randomUUID().toString().replace("-","_");
    final String testTable = "ml_test_" + testID;
    final String testResultColumn = "prediction_result";

    //TODO support error metric UDAFs
    TableTestingSpec spec = TableTestingSpec.newBuilder()
      .hiveConf(conf)
      .database(database == null ? "default" : database)
      .table(table)
      .featureColumns(model.getFeatureColumns())
      .outputColumn(testResultColumn)
      .labeColumn(model.getLabelColumn())
      .algorithm(algorithm)
      .modelID(modelID)
      .outputTable(testTable)
      .build();
    String testQuery = spec.getTestQuery();

    if (testQuery == null) {
      throw new GrillException("Invalid test spec. "
        + "table=" + table + " algorithm=" + algorithm + " modelID=" + modelID);
    }

    LOG.info("Running test query " + testQuery);
    QueryHandle testQueryHandle = queryRunner.runQuery(testQuery);

    MLTestReport testReport = new MLTestReport();
    testReport.setReportID(testID);
    testReport.setAlgorithm(algorithm);
    testReport.setFeatureColumns(model.getFeatureColumns());
    testReport.setLabelColumn(model.getLabelColumn());
    testReport.setModelID(model.getId());
    testReport.setOutputColumn(testResultColumn);
    testReport.setOutputTable(testTable);
    testReport.setTestTable(table);
    testReport.setQueryID(testQueryHandle.toString());

    // Save test report
    persistTestReport(testReport);
    LOG.info("Saved test report " + testReport.getReportID());
    return testReport;
  }

  private void persistTestReport(MLTestReport testReport) throws GrillException {
    LOG.info("saving test report " + testReport.getReportID());
    try {
      ModelLoader.saveTestReport(conf, testReport);
      LOG.info("Saved report " + testReport.getReportID());
    } catch (IOException e) {
      LOG.error("Error saving report " + testReport.getReportID() + " reason: " + e.getMessage());
    }
  }

  public List<String> getTestReports(String algorithm) throws GrillException {
    Path reportBaseDir = new Path(conf.get(ModelLoader.TEST_REPORT_BASE_DIR,
      ModelLoader.TEST_REPORT_BASE_DIR_DEFAULT));
    FileSystem fs = null;

    try {
      fs = reportBaseDir.getFileSystem(conf);
      if (!fs.exists(reportBaseDir)) {
        return null;
      }

      Path algoDir = new Path(reportBaseDir, algorithm);
      if (!fs.exists(algoDir)) {
        return null;
      }

      List<String> reports = new ArrayList<String>();
      for (FileStatus stat : fs.listStatus(algoDir)) {
        reports.add(stat.getPath().getName());
      }
      return reports;
    } catch (IOException e) {
      LOG.error("Error reading report list for " + algorithm, e);
      return null;
    }
  }

  public MLTestReport getTestReport(String algorithm, String reportID) throws GrillException {
    try {
      return ModelLoader.loadReport(conf, algorithm, reportID);
    } catch (IOException e) {
      throw new GrillException(e);
    }
  }

  public Object predict(String algorithm, String modelID, Object[] features) throws GrillException {
    // Load the model instance
    MLModel<?> model = getModel(algorithm, modelID);
    return model.predict(features);
  }

  public void deleteModel(String algorithm, String modelID) throws GrillException {
    try {
      ModelLoader.deleteModel(conf, algorithm, modelID);
      LOG.info("DELETED model " + modelID + " algorithm=" + algorithm);
    } catch (IOException e) {
      LOG.error("Error deleting model file. algorithm="
        + algorithm + " model=" + modelID + " reason: " + e.getMessage(), e);
      throw new GrillException("Unable to delete model " + modelID +" for algorithm " + algorithm, e);
    }
  }

  public void deleteTestReport(String algorithm, String reportID) throws GrillException {
    try {
      ModelLoader.deleteTestReport(conf, algorithm, reportID);
      LOG.info("DELETED report=" + reportID + " algorithm=" + algorithm);
    } catch (IOException e) {
      LOG.error("Error deleting report " + reportID + " algorithm=" + algorithm + " reason: " + e.getMessage(), e);
      throw new GrillException("Unable to delete report " + reportID + " for algorithm " + algorithm, e);
    }
  }

  public Map<String, String> getAlgoParamDescription(String algorithm) {
    MLTrainer trainer = null;
    try {
      trainer = getTrainerForName(algorithm);
    } catch (GrillException e) {
      LOG.error("Error getting algo description : " + algorithm, e);
      return null;
    }
    if (trainer instanceof BaseSparkTrainer) {
      return ((BaseSparkTrainer) trainer).getArgUsage();
    }
    return null;
  }

  /**
   * Submit model test query to a remote grill server
   */
  class RemoteQueryRunner extends TestQueryRunner {
    final String queryApiUrl;

    public RemoteQueryRunner(GrillSessionHandle sessionHandle, String queryApiUrl) {
      super(sessionHandle);
      this.queryApiUrl = queryApiUrl;
    }

    @Override
    public QueryHandle runQuery(String query) throws GrillException {
      // Create jersey client for query endpoint
      Client client = ClientBuilder
        .newBuilder()
        .register(MultiPartFeature.class)
        .build();
      WebTarget target = client.target(queryApiUrl);
      final FormDataMultiPart mp = new FormDataMultiPart();
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("sessionid").build(),
        sessionHandle, MediaType.APPLICATION_XML_TYPE));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name("query").build(),
        query));
      mp.bodyPart(new FormDataBodyPart(FormDataContentDisposition.name(
        "operation").build(),
        "execute"));

      GrillConf grillConf = new GrillConf();
      grillConf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, false + "");
      grillConf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false + "");
      mp.bodyPart(new FormDataBodyPart(
        FormDataContentDisposition.name("conf").fileName("conf").build(),
        grillConf,
        MediaType.APPLICATION_XML_TYPE));

      final QueryHandle handle = target.request().post(
        Entity.entity(mp, MediaType.MULTIPART_FORM_DATA_TYPE), QueryHandle.class);

      GrillQuery ctx = target.path(handle.toString())
        .queryParam("sessionid", sessionHandle)
        .request()
        .get(GrillQuery.class);

      QueryStatus stat = ctx.getStatus();
      while (!stat.isFinished()) {
        ctx = target.path(handle.toString())
          .queryParam("sessionid", sessionHandle)
          .request().get(GrillQuery.class);
        stat = ctx.getStatus();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          throw new GrillException(e);
        }
      }

      if (stat.getStatus() != QueryStatus.Status.SUCCESSFUL) {
        throw new GrillException("Query failed " + ctx.getQueryHandle().getHandleId()
          + " reason:" + stat.getErrorMessage());
      }

      return ctx.getQueryHandle();
    }
  }

  private GrillConf toGrillConf(HiveConf conf) {
    GrillConf grillConf = new GrillConf();
    grillConf.getProperties().putAll(conf.getValByRegex(".*"));
    return grillConf;
  }
}
