package org.apache.lens.ml.spark.trainers;

import org.apache.lens.api.LensException;
import org.apache.lens.ml.spark.models.BaseSparkClassificationModel;
import org.apache.lens.ml.spark.models.LogitRegressionClassificationModel;
import org.apache.lens.ml.Algorithm;
import org.apache.lens.ml.TrainerParam;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.util.Map;

@Algorithm(
  name = "spark_logistic_regression",
  description = "Spark logistic regression trainer"
)
public class LogisticRegressionTrainer extends BaseSparkTrainer {
  @TrainerParam(name = "iterations", help ="Max number of iterations",
  defaultValue = "100")
  private int iterations;

  @TrainerParam(name = "stepSize", help = "Step size", defaultValue = "1.0d")
  private double stepSize;

  @TrainerParam(name = "minBatchFraction", help = "Fraction for batched learning",
  defaultValue = "1.0d")
  private double minBatchFraction;

  public LogisticRegressionTrainer(String name, String description) {
    super(name, description);
  }

  @Override
  public void parseTrainerParams(Map<String, String> params) {
    iterations = getParamValue("iterations", 100);
    stepSize = getParamValue("stepSize", 1.0d);
    minBatchFraction = getParamValue("minBatchFraction", 1.0d);
  }

  @Override
  protected BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD) throws LensException {
    LogisticRegressionModel lrModel =
      LogisticRegressionWithSGD.train(trainingRDD, iterations, stepSize, minBatchFraction);
    return new LogitRegressionClassificationModel(modelId, lrModel);
  }
}
