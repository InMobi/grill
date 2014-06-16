package com.inmobi.grill.server.api.ml;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public interface MLModel extends Serializable {
  public double predict(Object ... args);
  public String getId();
  public Date getCreatedAt();
  public String getTrainerName();
  public String getTable();
  public List<String> getParams();
  public List<String> getFeatureColumns();
  public String getLabelColumn();
}
