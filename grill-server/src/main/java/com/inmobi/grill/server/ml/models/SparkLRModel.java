package com.inmobi.grill.server.ml.models;

import com.inmobi.grill.server.ml.MLModel;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Arrays;

public class SparkLRModel implements MLModel {
  private String modelId;
  LogisticRegressionModel sparkLRModel;

  public SparkLRModel(String modelId, LogisticRegressionModel sparkLRModel) {
    this.modelId = modelId;
    this.sparkLRModel = sparkLRModel;
  }

  @Override
  public double predict(Object... args) {
    double[] features = new double[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] instanceof  Double) {
        features[i] = (Double) args[i];
      } else {
        features[i] = 0.0;
      }
    }
    return sparkLRModel.predict(Vectors.dense(features));
  }

  @Override
  public String getId() {
    return modelId;
  }
}
