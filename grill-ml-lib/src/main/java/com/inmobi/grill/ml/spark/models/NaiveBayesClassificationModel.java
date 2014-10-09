package com.inmobi.grill.ml.spark.models;

import org.apache.spark.mllib.classification.NaiveBayesModel;

public class NaiveBayesClassificationModel extends BaseSparkClassificationModel<NaiveBayesModel> {
  public NaiveBayesClassificationModel(String modelId, NaiveBayesModel model) {
    super(modelId, model);
  }
}
