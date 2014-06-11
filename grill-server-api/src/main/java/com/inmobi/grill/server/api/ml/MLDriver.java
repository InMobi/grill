package com.inmobi.grill.server.api.ml;

import com.inmobi.grill.api.GrillException;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public interface MLDriver {
  public boolean isTrainerSupported(String trainer);
  public MLTrainer getTrainerInstance(String trainer) throws GrillException;
  public void init(Configuration conf) throws GrillException;
  public void start() throws GrillException;
  public void stop() throws GrillException;
  public List<String> getTrainerNames();
}
