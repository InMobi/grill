package com.inmobi.grill.ml.spark;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.ml.Algorithms;
import com.inmobi.grill.ml.MLDriver;
import com.inmobi.grill.ml.MLTrainer;
import com.inmobi.grill.ml.spark.trainers.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;


public class SparkMLDriver implements MLDriver {
  public static final Log LOG = LogFactory.getLog(SparkMLDriver.class);
  private boolean ownsSparkContext = true;

  private enum SparkMasterMode {
    // Embedded mode used in tests
    EMBEDDED,
    // Yarn client and Yarn cluster modes are used when deploying the app to Yarn cluster
    YARN_CLIENT,
    YARN_CLUSTER
  }

  private final Algorithms algorithms = new Algorithms();
  private SparkMasterMode clientMode = SparkMasterMode.EMBEDDED;
  private boolean isStarted;
  private SparkConf sparkConf;
  private JavaSparkContext sparkContext;

  public void useSparkContext(JavaSparkContext jsc) {
    ownsSparkContext = false;
    this.sparkContext = jsc;
  }

  @Override
  public boolean isTrainerSupported(String name) {
    return algorithms.isAlgoSupported(name);
  }

  @Override
  public MLTrainer getTrainerInstance(String name) throws GrillException {
    checkStarted();

    if (!isTrainerSupported(name)) {
      return null;
    }

    MLTrainer trainer = null;
    try {
      trainer = algorithms.getTrainerForName(name);
      if (trainer instanceof BaseSparkTrainer) {
        ((BaseSparkTrainer) trainer).setSparkContext(sparkContext);
      }
    } catch (GrillException exc) {
      LOG.error("Error creating trainer object", exc);
    }
    return trainer;
  }

  private void registerTrainers() {
    algorithms.register(NaiveBayesTrainer.class);
    algorithms.register(SVMTrainer.class);
    algorithms.register(LogisticRegressionTrainer.class);
    algorithms.register(DecisionTreeTrainer.class);
  }

  @Override
  public void init(GrillConf conf) throws GrillException {
    sparkConf = new SparkConf();
    registerTrainers();
    for (String key : conf.getProperties().keySet()) {
      if (key.startsWith("grill.ml.sparkdriver.")) {
        sparkConf.set(key.substring("grill.ml.sparkdriver.".length()),
          conf.getProperties().get(key));
      }
    }

    String sparkHome = System.getenv("SPARK_HOME");
    if (StringUtils.isNotBlank(sparkHome)) {
      sparkConf.setSparkHome(sparkHome);
    }

    // If SPARK_HOME is not set, SparkConf can read from the grill-site.xml or System properties.
    if (StringUtils.isBlank(sparkConf.get("spark.home"))) {
      throw new GrillException("Spark home is not set");
    }

    String sparkAppMaster = sparkConf.get("spark.master");
    if ("yarn-client".equalsIgnoreCase(sparkAppMaster)) {
      clientMode = SparkMasterMode.YARN_CLIENT;
    } else if ("yarn-cluster".equalsIgnoreCase(sparkAppMaster)) {
      clientMode = SparkMasterMode.YARN_CLUSTER;
    } else if ("local".equalsIgnoreCase(sparkAppMaster) || StringUtils.isBlank(sparkAppMaster)) {
      clientMode = SparkMasterMode.EMBEDDED;
    }
    sparkConf.setAppName("grill-ml");
    LOG.info("Spark home is set to " + sparkConf.get("spark.home"));
  }

  @Override
  public void start() throws GrillException {
    if (sparkContext == null) {
      sparkContext = new JavaSparkContext(sparkConf);
    }


    // Adding jars to spark context is only required when running in yarn-client mode
    if (clientMode != SparkMasterMode.EMBEDDED) {
      // TODO Figure out only necessary set of JARs to be added for HCatalog
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
    if (ownsSparkContext) {
      sparkContext.stop();
    }
    LOG.info("Stopped spark context " + this);
  }

  @Override
  public List<String> getTrainerNames() {
    return algorithms.getAlgorithmNames();
  }

  public void checkStarted() throws GrillException {
    if (!isStarted) {
      throw new GrillException("Spark driver is not started yet");
    }
  }

  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }

}
