package com.inmobi.grill.server.ml;

import com.inmobi.grill.server.api.ml.MLModel;

import java.util.Date;

public abstract class BaseModel implements MLModel {
  private String trainerName;
  private Date createdAt;

  public final double[] getFeatureVector(Object[] args) {
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

  protected void setTrainerName(String trainerName) {
    this.trainerName = trainerName;
  }

  @Override
  public String getTrainerName() {
    return trainerName;
  }

  @Override
  public Date getCreatedAt() {
    return createdAt;
  }

  protected void setCreatedAt(Date date) {
    this.createdAt = date;
  }
}
