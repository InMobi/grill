package com.inmobi.grill.server.api.ml;

import com.inmobi.grill.api.GrillException;
import org.apache.hadoop.conf.Configuration;


public interface MLTrainer {
  public String getName();
  public String getDescription();
  public void configure(Configuration configuration);
  public Configuration getConf();
  public MLModel train(Configuration conf, String db, String table, String modelId, String ... params) throws GrillException;
}
