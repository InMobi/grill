package com.inmobi.grill.server.ml;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.ml.*;
import com.inmobi.grill.server.api.query.QueryExecutionService;
import com.inmobi.grill.server.ml.spark.TableTestingSpec;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.service.cli.CLIService;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

public class MLServiceImpl extends GrillService implements MLService {
  public static final Log LOG = LogFactory.getLog(MLServiceImpl.class);
  protected List<MLDriver> drivers;
  private HiveConf conf;

  public MLServiceImpl(String name, CLIService cliService) {
    super(NAME, cliService);
  }

  public MLServiceImpl(CLIService cliService) {
    this(NAME, cliService);
  }

  protected HiveConf getConf() {
    return conf;
  }

  @Override
  public List<String> getAlgorithms() {
    List<String> trainers = new ArrayList<String>();
    for (MLDriver driver : drivers) {
      trainers.addAll(driver.getTrainerNames());
    }
    return trainers;
  }

  @Override
  public MLTrainer getTrainerForName(String algorithm) throws GrillException {
    for (MLDriver driver : drivers) {
      if (driver.isTrainerSupported(algorithm)) {
        return driver.getTrainerInstance(algorithm);
      }
    }
    throw new GrillException("Trainer not supported " + algorithm);
  }

  @Override
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

    MLModel model = trainer.train(getHiveConf(), database, table, modelId, args);

    LOG.info("Done training model: " + modelId);

    if (model instanceof BaseModel) {
      ((BaseModel) model).setCreatedAt(new Date());
      ((BaseModel) model).setTrainerName(algorithm);
    }

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


  @Override
  public List<String> getModels(String algorithm) throws GrillException {
    try {
      Path trainerDir = getTrainerDir(algorithm);
      FileSystem fs = trainerDir.getFileSystem(conf);
      if (!fs.exists(trainerDir)) {
        throw new GrillException("Models not found for trainer: " + algorithm);
      }

      List<String> models = new ArrayList<String>();

      for (FileStatus stat : fs.listStatus(trainerDir)) {
        models.add(stat.getPath().getName());
      }

      if (models.isEmpty()) {
        throw new GrillException("No models found for trainer " + algorithm);
      }

      return models;
    } catch (IOException ioex) {
      throw new GrillException(ioex);
    }
  }

  @Override
  public MLModel getModel(String algorithm, String modelId) throws GrillException {
    try {
      return ModelLoader.loadModel(new JobConf(conf), algorithm, modelId);
    } catch (IOException e) {
      throw new GrillException(e);
    }
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.conf = hiveConf;

    // Get all the drivers
    String[] driverClasses = hiveConf.getStrings("grill.ml.drivers");
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
        driver.init(hiveConf);
        drivers.add(driver);
        LOG.info("Added driver " + driverClass);
      } catch (Exception e) {
        LOG.error("Failied to create driver " + driverClass + " reason: " + e.getMessage() , e);
      }
    }
    super.init(hiveConf);
    LOG.info("Inited ML service");
  }

  @Override
  public synchronized void start() {
    for (MLDriver driver : drivers) {
      try {
        driver.start();
      } catch (GrillException e) {
        LOG.error("Failed to start driver " + driver, e);
      }
    }
    super.start();
    LOG.info("Started ML service");
  }

  @Override
  public synchronized void stop() {
    for (MLDriver driver : drivers) {
      try {
        driver.stop();
      } catch (GrillException e) {
        LOG.error("Failed to stop driver " + driver, e);
      }
    }
    drivers.clear();
    super.stop();
    LOG.info("Stopped ML service");
  }

  @Override
  public synchronized HiveConf getHiveConf() {
    return conf;
  }

  public void clearModels() {
    ModelLoader.clearCache();
  }

  @Override
  public String getModelPath(String algorithm, String modelID) {
    return ModelLoader.getModelLocation(conf, algorithm, modelID).toString();
  }

  @Override
  public MLTestReport testModel(GrillSessionHandle sessionHandle,
                                String table,
                                String algorithm,
                                String modelID) throws GrillException {
    // check if algorithm exists
    if (!getAlgorithms().contains(algorithm)) {
      throw new GrillException("No such algorithm " + algorithm);
    }

    MLModel model;
    try {
      model = ModelLoader.loadModel(new JobConf(conf), algorithm, modelID);
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

    // Run the query in query executions service
    QueryExecutionService queryService = (QueryExecutionService) GrillServices.get().getService("query");

    GrillConf queryConf = new GrillConf();
    queryConf.addProperty(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, false + "");

    QueryHandle testQueryHandle = queryService.executeAsync(sessionHandle,
      testQuery,
      queryConf
    );

    // Wait for test query to complete
    GrillQuery query = queryService.getQuery(sessionHandle, testQueryHandle);
    while (!query.getStatus().isFinished()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new GrillException(e);
      }

      query = queryService.getQuery(sessionHandle, testQueryHandle);
      LOG.info("Polling for test query " + testID + " grill query handle= " + testQueryHandle
        + " status: " + query.getStatus().getStatus());
    }

    if (query.getStatus().getStatus() != QueryStatus.Status.SUCCESSFUL) {
      throw new GrillException("Failed to run test " + algorithm
        + ", modelID=" + modelID + " table=" + table + " grill query: " + testQueryHandle
      + " reason= " + query.getStatus().getErrorMessage());
    }

    BaseTestReport testReport = new BaseTestReport();
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

  private void persistTestReport(BaseTestReport testReport) throws GrillException {
    LOG.info("saving test report " + testReport.getReportID());
  }

  @Override
  public List<MLTestReport> getTestReports(String algorithm) {
    return null;
  }

  @Override
  public MLTestReport getTestReport(String reportID) {
    return null;
  }

  @Override
  public Prediction predict(String algorithm, String modelID, Object[] features) throws GrillException {
    return null;
  }
}
