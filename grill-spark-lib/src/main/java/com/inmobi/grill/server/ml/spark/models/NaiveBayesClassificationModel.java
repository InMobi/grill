package com.inmobi.grill.server.ml.spark.models;

import com.inmobi.grill.server.ml.BaseModel;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;

public class NaiveBayesClassificationModel extends BaseSparkClassificationModel<NaiveBayesModel> {
  public NaiveBayesClassificationModel(String modelId, NaiveBayesModel model) {
    super(modelId, model);
  }
}
