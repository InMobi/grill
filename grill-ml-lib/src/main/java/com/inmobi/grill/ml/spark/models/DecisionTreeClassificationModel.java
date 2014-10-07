package com.inmobi.grill.ml.spark.models;



public class DecisionTreeClassificationModel extends BaseSparkClassificationModel<SparkDecisionTreeModel> {
  public DecisionTreeClassificationModel(String modelId, SparkDecisionTreeModel model) {
    super(modelId, model);
  }
}
