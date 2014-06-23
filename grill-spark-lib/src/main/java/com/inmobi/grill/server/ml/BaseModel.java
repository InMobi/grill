package com.inmobi.grill.server.ml;


import com.inmobi.grill.server.api.ml.MLModel;

import java.util.Date;
import java.util.List;

public abstract class BaseModel implements MLModel {
  private String trainerName;
  private Date createdAt;
  private List<String> params;
  private String table;
  private String labelColumn;
  private List<String> featureColumns;

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

  @Override
  public List<String> getParams() {
    return params;
  }

  public void setParams(List<String> params) {
    this.params = params;
  }

  @Override
  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  @Override
  public List<String> getFeatureColumns() {
    return featureColumns;
  }

  @Override
  public String getLabelColumn() {
    return labelColumn;
  }

  public void setLabelColumn(String labelColumn) {
    this.labelColumn = labelColumn;
  }

  public void setFeatureColumns(List<String> featureColumns) {
    this.featureColumns = featureColumns;
  }
}
