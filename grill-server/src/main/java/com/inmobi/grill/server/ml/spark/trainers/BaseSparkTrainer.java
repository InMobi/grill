package com.inmobi.grill.server.ml.spark.trainers;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.api.ml.MLModel;
import com.inmobi.grill.server.api.ml.MLTrainer;
import com.inmobi.grill.server.ml.spark.TableTrainingSpec;
import com.inmobi.grill.server.ml.spark.models.BaseSparkClassificationModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.util.*;

public abstract class BaseSparkTrainer implements MLTrainer {
  public static final Log LOG = LogFactory.getLog(BaseSparkTrainer.class);

  protected JavaSparkContext sparkContext;
  protected Map<String, String> params;
  protected transient HiveConf conf;
  private double trainingFraction;
  private boolean useTrainingFraction;
  protected String label;
  protected String partitionFilter;
  protected List<String> features;

  public void setSparkContext(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  @Override
  public HiveConf getConf() {
    return conf;
  }

  @Override
  public void configure(Configuration configuration) {
    this.conf = new HiveConf(configuration, BaseSparkTrainer.class);
  }

  @Override
  public MLModel train(Configuration conf, String db, String table, String modelId, String... params) throws GrillException {
    parseParams(params);
    LOG.info("Training " + " with " + features.size() + " features");
    TableTrainingSpec.TableTrainingSpecBuilder builder =
      TableTrainingSpec.newBuilder()
        .hiveConf(new HiveConf(conf, BaseSparkTrainer.class))
        .database(db)
        .table(table)
        .partitionFilter(partitionFilter)
        .featureColumns(features)
        .labelColumn(label);

    if (useTrainingFraction) {
      builder.trainingFraction(trainingFraction);
    }

    TableTrainingSpec spec = builder.build();
    spec.createRDDs(sparkContext);

    RDD<LabeledPoint> trainingRDD = spec.getTrainingRDD();
    BaseSparkClassificationModel model = trainInternal(modelId, trainingRDD);
    model.setTable(table);
    model.setParams(Arrays.asList(params));
    model.setLabelColumn(label);
    model.setFeatureColumns(features);
    return model;
  }

  public void parseParams(String[] args) {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException("Invalid number of params " + args.length);
    }

    params = new LinkedHashMap<String, String>();

    for (int i = 0; i < args.length; i+=2) {
      if ("f".equalsIgnoreCase(args[i]) || "feature".equalsIgnoreCase(args[i])) {
        if (features == null) {
          features = new ArrayList<String>();
        }
        features.add(args[i+1]);
      } else if ("l".equalsIgnoreCase(args[i]) || "label".equalsIgnoreCase(args[i])) {
        label = args[i+1];
      } else {
        params.put(args[i].replaceAll("\\-+", ""), args[i + 1]);
      }
    }

    if (StringUtils.isBlank(label)) {
      throw new IllegalArgumentException("Label column not provided");
    }

    if (features == null || features.isEmpty()) {
      throw new IllegalArgumentException("At least one feature column is required");
    }

    if (params.containsKey("trainingFraction")) {
      // Get training Fraction
      String trainingFractionStr = params.get("trainingFraction");
      try {
        trainingFraction = Double.parseDouble(trainingFractionStr);
        useTrainingFraction = true;
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Invalid training fraction", nfe);
      }
    }

    if (params.containsKey("partition") || params.containsKey("p")) {
      partitionFilter = params.containsKey("partition") ? params.get("partition") : params.get("p");
    }

    parseTrainerParams(params);
  }

  public double getParamValue(String param, double defaultVal) {
    if (params.containsKey(param)) {
      try {
        return Double.parseDouble(params.get(param));
      } catch (NumberFormatException nfe) {
      }
    }
    return defaultVal;
  }

  public int getParamValue(String param, int defaultVal) {
    if (params.containsKey(param)) {
      try {
        return Integer.parseInt(params.get(param));
      } catch (NumberFormatException nfe) {
      }
    }
    return defaultVal;
  }

  public abstract void parseTrainerParams(Map<String, String> params);
  protected abstract BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD) throws GrillException;
}
