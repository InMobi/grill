package com.inmobi.grill.server.ml.spark.trainers;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.ml.MLModel;
import com.inmobi.grill.server.ml.spark.models.LogitRegressionClassificationModel;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.util.Map;

public class LogisticRegressionTrainer extends BaseSparkTrainer {
  private int iterations;
  private double stepSize;
  private double minBatchFraction;

  public LogisticRegressionTrainer(String name, String description, JavaSparkContext sparkContext) {
    super(name, description, sparkContext);
  }

  @Override
  public void parseTrainerParams(Map<String, String> params) {
    iterations = getParamValue("iterations", 100);
    stepSize = getParamValue("stepSize", 1.0d);
    minBatchFraction = getParamValue("minBatchFraction", 1.0d);
  }

  @Override
  protected MLModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD) throws GrillException {
    LogisticRegressionModel lrModel =
      LogisticRegressionWithSGD.train(trainingRDD, iterations, stepSize, minBatchFraction);
    return new LogitRegressionClassificationModel(modelId, lrModel);
  }

}
