package com.inmobi.grill.server.ml;

public abstract class BaseModel implements MLModel {
  public final double[] getFeatureVector(Object ... args) {
    double[] features = new double[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] instanceof  Double) {
        features[i] = (Double) args[i];
      } else {
        features[i] = 0.0;
      }
    }
    return features;
  }
}
