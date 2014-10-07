package com.inmobi.grill.ml.spark.models;

import org.apache.spark.mllib.classification.LogisticRegressionModel;

public class LogitRegressionClassificationModel extends BaseSparkClassificationModel<LogisticRegressionModel> {
  public LogitRegressionClassificationModel(String modelId, LogisticRegressionModel model) {
    super(modelId, model);
  }
}