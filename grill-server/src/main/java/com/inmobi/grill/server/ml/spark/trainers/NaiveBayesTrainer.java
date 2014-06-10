package com.inmobi.grill.server.ml.spark.trainers;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.ml.MLModel;
import com.inmobi.grill.server.ml.spark.models.NaiveBayesClassificationModel;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import scala.tools.cmd.gen.AnyVals;

import java.util.Map;

public class NaiveBayesTrainer extends BaseSparkTrainer {
  public static final String NAME = "spark_naive_bayes";
  public static final String DESCRIPTION = "Spark Naive Bayes classifier trainer";
  private double lambda = 1.0;

  public NaiveBayesTrainer(JavaSparkContext sparkContext) {
    super(NAME, DESCRIPTION, sparkContext);
  }

  @Override
  public void parseTrainerParams(Map<String, String> params) {
    lambda = getParamValue("lambda", 1.0d);
  }

  @Override
  protected MLModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD) throws GrillException {
    return new NaiveBayesClassificationModel(modelId, NaiveBayes.train(trainingRDD, lambda));
  }
}
