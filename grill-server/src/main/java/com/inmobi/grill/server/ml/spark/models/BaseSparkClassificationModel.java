package com.inmobi.grill.server.ml.spark.models;

import com.inmobi.grill.server.ml.BaseModel;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.linalg.Vectors;

public class BaseSparkClassificationModel<MODEL extends ClassificationModel> extends BaseModel {
  private final String modelId;
  private final MODEL sparkModel;

  public BaseSparkClassificationModel(String modelId, MODEL model) {
    this.modelId = modelId;
    this.sparkModel = model;
  }

  @Override
  public double predict(Object... args) {
    return sparkModel.predict(Vectors.dense(getFeatureVector(args)));
  }

  @Override
  public String getId() {
    return modelId;
  }

}