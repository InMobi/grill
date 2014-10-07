package com.inmobi.grill.ml.spark;

import com.inmobi.grill.ml.spark.FeatureValueMapper;

/**
 * Directly return input when it is known to be double
 */
public class DoubleValueMapper extends FeatureValueMapper {
  @Override
  public final Double call(Object input) {
    return input == null ? 0d : (Double) input;
  }
}
