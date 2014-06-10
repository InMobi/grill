package com.inmobi.grill.server.ml.spark.trainers;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTrainerFactory {
  public static BaseSparkTrainer getTrainer(JavaSparkContext context, String trainerName) {
    Preconditions.checkNotNull(context, "JavaSparkContext is required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(trainerName), "Trainer name is required. found: "
      +trainerName);

    if (NaiveBayesTrainer.NAME.equalsIgnoreCase(trainerName)) {
      return new NaiveBayesTrainer(context);
    } else if (SVMTrainer.NAME.equalsIgnoreCase(trainerName)) {
      return new SVMTrainer(context);
    } else if (LogisticRegressionTrainer.NAME.equalsIgnoreCase(trainerName)) {
      return new LogisticRegressionTrainer(context);
    }

    throw new IllegalArgumentException("Unknown trainer " + trainerName);
  }

}
