package com.inmobi.grill.server.api.ml;

import java.io.Serializable;
import java.util.Date;

public interface MLModel extends Serializable {
  public double predict(Object ... args);
  public String getId();
  public Date getCreatedAt();
  public String getTrainerName();
  public String getTable();
}
