package com.inmobi.grill.ml;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;

import java.util.List;

public interface MLDriver {
  public boolean isTrainerSupported(String trainer);
  public MLTrainer getTrainerInstance(String trainer) throws GrillException;
  public void init(GrillConf conf) throws GrillException;
  public void start() throws GrillException;
  public void stop() throws GrillException;
  public List<String> getTrainerNames();
}
