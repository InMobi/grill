package com.inmobi.grill.server.ml;

import com.inmobi.grill.api.GrillException;
import org.apache.hadoop.hive.conf.HiveConf;


public interface MLTrainer {
  public String getName();
  public String getDescription();
  public void configure(HiveConf configuration);
  public HiveConf getConf();
  public MLModel train(HiveConf conf, String db, String table, String modelId, String ... params) throws GrillException;
}
