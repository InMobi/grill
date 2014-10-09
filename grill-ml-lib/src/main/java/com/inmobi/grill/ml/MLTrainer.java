package com.inmobi.grill.ml;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;


public interface MLTrainer {
  public String getName();
  public String getDescription();
  public void configure(GrillConf configuration);
  public GrillConf getConf();
  public MLModel train(GrillConf conf, String db, String table, String modelId, String ... params) throws GrillException;
}
