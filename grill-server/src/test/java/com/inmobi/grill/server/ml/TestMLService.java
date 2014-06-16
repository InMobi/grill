package com.inmobi.grill.server.ml;

import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.ml.MLDriver;
import com.inmobi.grill.server.api.ml.MLService;
import com.inmobi.grill.server.api.ml.MLTrainer;
import com.inmobi.grill.server.ml.spark.SparkMLDriver;
import com.inmobi.grill.server.ml.spark.trainers.LogisticRegressionTrainer;
import com.inmobi.grill.server.ml.spark.trainers.NaiveBayesTrainer;
import com.inmobi.grill.server.ml.spark.trainers.SVMTrainer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.testng.Assert.*;

@Test(groups = "ml")
public class TestMLService {
  public static final Log LOG = LogFactory.getLog(TestMLService.class);

  @Test
  public void testMLService() throws Exception {
    LOG.info("@@ MLService test");
    // Needed to load grill-site.xml and grill-default.xml
    GrillServices services = new GrillServices(GrillServices.GRILL_SERVICES_NAME);

    MLServiceImpl service = new MLServiceImpl(MLService.NAME, null);
    HiveConf conf = new HiveConf();

    service.init(conf);
    assertEquals(service.getServiceState(), Service.STATE.INITED);
    LOG.info("@@ ML service inited");

    service.start();
    assertEquals(service.getServiceState(), Service.STATE.STARTED);
    LOG.info("@@ ML service started");

    assertNotNull(service.drivers);
    assertEquals(service.drivers.size(), 1);

    MLDriver driver = service.drivers.get(0);
    assertTrue(driver instanceof SparkMLDriver);

    SparkMLDriver sparkDriver = (SparkMLDriver) driver;
    sparkDriver.checkStarted();
    assertNotNull(sparkDriver.getSparkContext());

    List<String> trainers = service.getAlgorithms();
    assertNotNull(trainers);
    assertEquals(trainers.size(), 3);
    assertEquals(new HashSet<String>(trainers),
      new HashSet<String>(Arrays.asList(SVMTrainer.NAME, NaiveBayesTrainer.NAME, LogisticRegressionTrainer.NAME)));

    MLTrainer trainer = service.getTrainerForName(SVMTrainer.NAME);
    assertNotNull(trainer);
    assertTrue(trainer instanceof SVMTrainer);

    MLTrainer trainer2 = service.getTrainerForName(NaiveBayesTrainer.NAME);
    assertNotNull(trainer2);
    assertTrue(trainer2 instanceof NaiveBayesTrainer);

    MLTrainer trainer3 = service.getTrainerForName(LogisticRegressionTrainer.NAME);
    assertNotNull(trainer3);
    assertTrue(trainer3 instanceof LogisticRegressionTrainer);

    service.stop();
    try {
      sparkDriver.checkStarted();
      fail("Expecting spark driver to be stopped");
    } catch (Exception exc) {
      // pass
    }

    assertEquals(service.getServiceState(), Service.STATE.STOPPED);
    LOG.info("@@ MLService test done");
  }

}
