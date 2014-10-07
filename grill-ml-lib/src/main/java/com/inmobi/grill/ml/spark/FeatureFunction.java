package com.inmobi.grill.ml.spark;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

/**
 * Function to map an HCatRecord to a feature vector usable by MLLib
 */
public abstract class FeatureFunction implements Function<Tuple2<WritableComparable, HCatRecord>, LabeledPoint> {
  @Override
  public abstract LabeledPoint call(Tuple2<WritableComparable, HCatRecord> tuple) throws Exception;
}
