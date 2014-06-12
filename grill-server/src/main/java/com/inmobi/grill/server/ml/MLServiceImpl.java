package com.inmobi.grill.server.ml;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.api.ml.MLDriver;
import com.inmobi.grill.server.api.ml.MLModel;
import com.inmobi.grill.server.api.ml.MLService;
import com.inmobi.grill.server.api.ml.MLTrainer;
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

  @Override
  public List<String> getTrainerNames() {
    List<String> trainers = new ArrayList<String>();
    for (MLDriver driver : drivers) {
      trainers.addAll(driver.getTrainerNames());
    }
    return trainers;
  }

  @Override
  public MLTrainer getTrainerForName(String trainerName) throws GrillException {
    for (MLDriver driver : drivers) {
      if (driver.isTrainerSupported(trainerName)) {
        return driver.getTrainerInstance(trainerName);
      }
    }
    throw new GrillException("Trainer not supported " + trainerName);
  }

  @Override
  public String train(String table, String trainerName, String[] args) throws GrillException {
    MLTrainer trainer = getTrainerForName(trainerName);

    String modelId = UUID.randomUUID().toString();

    LOG.info("Begin training model " + modelId + ", trainer=" + trainerName + ", table=" + table + ", params="
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
      ((BaseModel) model).setTrainerName(trainerName);
    }

    Path modelLocation = null;
    try {
      modelLocation = persistModel(model);
      LOG.info("Model saved: " + modelId + ", trainer: " + trainerName + ", path: " + modelLocation);
      return model.getId();
    } catch (IOException e) {
      throw new GrillException("Error saving model " + modelId + " for trainer " + trainerName, e);
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
  public List<String> getModels(String trainer) throws GrillException {
    try {
      Path trainerDir = getTrainerDir(trainer);
      FileSystem fs = trainerDir.getFileSystem(conf);
      if (!fs.exists(trainerDir)) {
        throw new GrillException("Models not found for trainer: " + trainer);
      }

      List<String> models = new ArrayList<String>();

      for (FileStatus stat : fs.listStatus(trainerDir)) {
        models.add(stat.getPath().getName());
      }

      if (models.isEmpty()) {
        throw new GrillException("No models found for trainer " + trainer);
      }

      return models;
    } catch (IOException ioex) {
      throw new GrillException(ioex);
    }
  }

  @Override
  public MLModel getModel(String trainer, String modelId) throws GrillException {
    try {
      return ModelLoader.loadModel(new JobConf(conf), trainer, modelId);
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
}
