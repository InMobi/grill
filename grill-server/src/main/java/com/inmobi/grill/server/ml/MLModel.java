package com.inmobi.grill.server.ml;

import java.io.Serializable;

public interface MLModel extends Serializable {
  public double predict(Object ... args);
  public String getId();
}
