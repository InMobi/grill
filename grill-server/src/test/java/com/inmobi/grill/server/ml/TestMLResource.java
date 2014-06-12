package com.inmobi.grill.server.ml;

import com.inmobi.grill.api.StringList;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.ml.spark.MLApp;
import com.inmobi.grill.server.ml.spark.trainers.LogisticRegressionTrainer;
import com.inmobi.grill.server.ml.spark.trainers.NaiveBayesTrainer;
import com.inmobi.grill.server.ml.spark.trainers.SVMTrainer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;

import java.util.Arrays;
import java.util.HashSet;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

@Test(groups = "ml")
public class TestMLResource extends GrillJerseyTest {
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected int getTestPort() {
    return 9000;
  }

  @Override
  protected Application configure() {
    return new MLApp();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  @Test
  public void testGetTrainers() throws Exception {
    WebTarget target = target("ml").path("trainers");
    StringList trainers = target.request().get(StringList.class);
    assertNotNull(trainers);
    assertEquals(trainers.getElements().size(), 3);
    assertEquals(new HashSet<String>(trainers.getElements()),
      new HashSet<String>(Arrays.asList(NaiveBayesTrainer.NAME,
        SVMTrainer.NAME,
        LogisticRegressionTrainer.NAME)));
  }

}
