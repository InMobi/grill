package com.inmobi.grill.server.ml.spark;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.ml.MLModel;
import com.inmobi.grill.server.ml.models.SparkLRModel;
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

  private static final int DEFAULT_ITERATIONS = 100;
  private static final double DEFAULT_STEP_SIZE = 1.0;
  private static final double DEFAULT_MIN_BATCH_FRACTION = 1.0;

  public LogisticRegressionTrainer(String name, String description, JavaSparkContext sparkContext) {
    super(name, description, sparkContext);
  }

  @Override
  public void parseTrainerParams(Map<String, String> params) {
    if (params.containsKey("iterations")) {
      iterations = Integer.parseInt(params.get("iterations"));
    } else {
      iterations = DEFAULT_ITERATIONS;
    }

    if (params.containsKey("stepSize")) {
      stepSize = Double.parseDouble(params.get("stepSize"));
    } else {
      stepSize = DEFAULT_STEP_SIZE;
    }

    if (params.containsKey("minBatchFraction")) {
      minBatchFraction = Double.parseDouble(params.get("minBatchFraction"));
    } else {
      minBatchFraction = DEFAULT_MIN_BATCH_FRACTION;
    }
  }

  @Override
  protected MLModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD) throws GrillException {
    LogisticRegressionModel lrModel =
      LogisticRegressionWithSGD.train(trainingRDD, iterations, stepSize, minBatchFraction);
    return new SparkLRModel(modelId, lrModel);
  }

}
