package com.inmobi.grill.server.api.ml;

import com.inmobi.grill.api.GrillException;

import java.util.List;

public interface MLService {
  public static final String NAME = "ml";
  public List<String> getTrainerNames();
  public MLTrainer getTrainerForName(String trainerName) throws GrillException;
  public String train(String table, String trainerName, String[] args) throws GrillException;
  public List<String> getModels(String trainer) throws GrillException;
  public MLModel getModel(String trainer, String modelId) throws GrillException;
  String getModelPath(String algoName, String modelID);
}
