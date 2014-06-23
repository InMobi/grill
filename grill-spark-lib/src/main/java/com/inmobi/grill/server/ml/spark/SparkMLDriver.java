package com.inmobi.grill.server.ml.spark;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.api.ml.MLDriver;
import com.inmobi.grill.server.api.ml.MLTrainer;
import com.inmobi.grill.server.ml.spark.trainers.BaseSparkTrainer;
import com.inmobi.grill.server.ml.spark.trainers.LogisticRegressionTrainer;
import com.inmobi.grill.server.ml.spark.trainers.NaiveBayesTrainer;
import com.inmobi.grill.server.ml.spark.trainers.SVMTrainer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SparkMLDriver implements MLDriver {
  private boolean isStarted;

  public static final Log LOG = LogFactory.getLog(SparkMLDriver.class);

  private static final Map<String, Class<? extends BaseSparkTrainer>> trainerClasses =
    new HashMap<String, Class<? extends BaseSparkTrainer>>();

  static {
    trainerClasses.put(LogisticRegressionTrainer.NAME, LogisticRegressionTrainer.class);
    trainerClasses.put(NaiveBayesTrainer.NAME, NaiveBayesTrainer.class);
    trainerClasses.put(SVMTrainer.NAME, SVMTrainer.class);
  }

  private SparkConf sparkConf;
  private JavaSparkContext sparkContext;

  @Override
  public boolean isTrainerSupported(String name) {
    return trainerClasses.containsKey(name);
  }

  @Override
  public MLTrainer getTrainerInstance(String name) throws GrillException {
    checkStarted();

    if (!isTrainerSupported(name)) {
      return null;
    }

    BaseSparkTrainer trainer = null;
    try {
      trainer = trainerClasses.get(name).newInstance();
      trainer.setSparkContext(sparkContext);
    } catch (InstantiationException e) {
      LOG.error("Error creating trainer object", e);
    } catch (IllegalAccessException e) {
      LOG.error("Error creating trainer object", e);
    }
    return trainer;
  }

  @Override
  public void init(Configuration conf) throws GrillException {
    sparkConf = new SparkConf();

    Map<String, String> sparkDriverConf = conf.getValByRegex("grill\\.ml\\.sparkdriver\\..*");
    for (String key : sparkDriverConf.keySet()) {
      sparkConf.set(key.substring("grill.ml.sparkdriver.".length()), sparkDriverConf.get(key));
    }

    String sparkHome = System.getenv("SPARK_HOME");
    if (StringUtils.isNotBlank(sparkHome)) {
      sparkConf.setSparkHome(sparkHome);
    }

    // If SPARK_HOME is not set, SparkConf can read from the grill-site.xml or System properties.
    if (StringUtils.isBlank(sparkConf.get("spark.home"))) {
      throw new GrillException("Spark home is not set");
    }
    LOG.info("Spark home is set to " + sparkConf.get("spark.home"));
  }

  @Override
  public void start() throws GrillException {
    sparkContext = new JavaSparkContext(sparkConf);

    // Add hcatalog and hive jars
    String hiveLocation = System.getenv("HIVE_HOME");

    if (StringUtils.isBlank(hiveLocation)) {
      throw new GrillException("HIVE_HOME is not set");
    }

    LOG.info("HIVE_HOME at " + hiveLocation);

    File hiveLibDir = new File(hiveLocation, "lib");
    FilenameFilter jarFileFilter = new FilenameFilter() {
      @Override
      public boolean accept(File file, String s) {
        return s.endsWith(".jar");
      }
    };

    List<String> jarFiles = new ArrayList<String>();

    // Add hive jars
    for (File jarFile : hiveLibDir.listFiles(jarFileFilter)) {
      jarFiles.add(jarFile.getAbsolutePath());
      LOG.info("Adding HIVE jar " + jarFile.getAbsolutePath());
      sparkContext.addJar(jarFile.getAbsolutePath());
    }

    // Add hcatalog jars
    File hcatalogDir = new File(hiveLocation + "/hcatalog/share/hcatalog");
    for (File jarFile : hcatalogDir.listFiles(jarFileFilter)) {
      jarFiles.add(jarFile.getAbsolutePath());
      LOG.info("Adding HCATALOG jar " + jarFile.getAbsolutePath());
      sparkContext.addJar(jarFile.getAbsolutePath());
    }

    // Add the current jar
    String[] grillSparkLibJars = JavaSparkContext.jarOfClass(SparkMLDriver.class);
    for (String grillSparkJar : grillSparkLibJars) {
      LOG.info("Adding GRILL JAR " + grillSparkJar);
      sparkContext.addJar(grillSparkJar);
    }

    isStarted = true;
    LOG.info("Created Spark context for app: '" + sparkContext.appName()
      + "', Spark master: " + sparkContext.master());
  }

  @Override
  public void stop() throws GrillException {
    if (!isStarted) {
      LOG.warn("Spark driver was not started");
      return;
    }
    isStarted = false;
    sparkContext.stop();
    LOG.info("Stopped spark context " + this);
  }

  @Override
  public List<String> getTrainerNames() {
    return new ArrayList<String>(trainerClasses.keySet());
  }

  public void checkStarted() throws GrillException{
    if (!isStarted) {
      throw new GrillException("Spark driver is not started yet");
    }
  }

  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }

}
