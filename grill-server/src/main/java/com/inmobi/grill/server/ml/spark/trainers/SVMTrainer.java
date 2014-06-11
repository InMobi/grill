package com.inmobi.grill.server.ml.spark.trainers;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.api.ml.MLModel;
import com.inmobi.grill.server.ml.spark.models.SVMClassificationModel;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.util.Map;

public class SVMTrainer extends BaseSparkTrainer {
  public static final String NAME = "spark_svm";
  public static final String DESCRIPTION = "Spark SVM classifier trainer";
  private double minBatchFraction;
  private double regParam;
  private double stepSize;
  private int iterations;


  @Override
  public void parseTrainerParams(Map<String, String> params) {
    minBatchFraction = getParamValue("minBatchFraction", 1.0);
    regParam = getParamValue("regParam", 1.0);
    stepSize = getParamValue("stepSize", 1.0);
    iterations = getParamValue("iterations", 100);
  }

  @Override
  protected MLModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD) throws GrillException {
    SVMModel svmModel = SVMWithSGD.train(trainingRDD, iterations, stepSize, regParam, minBatchFraction);
    return new SVMClassificationModel(modelId, svmModel);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return DESCRIPTION;
  }
}
