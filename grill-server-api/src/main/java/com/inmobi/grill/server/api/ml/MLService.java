package com.inmobi.grill.server.api.ml;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;

import java.util.List;

public interface MLService {
  public static final String NAME = "ml";
  public List<String> getAlgorithms();
  public MLTrainer getTrainerForName(String algorithm) throws GrillException;
  public String train(String table, String algorithm, String[] args) throws GrillException;
  public List<String> getModels(String algorithm) throws GrillException;
  public MLModel getModel(String algorithm, String modelId) throws GrillException;
  String getModelPath(String algorithm, String modelID);
  public MLTestReport testModel(GrillSessionHandle session, String table, String algorithm, String modelID) throws GrillException;
  public List<String> getTestReports(String algorithm) throws GrillException;
  public MLTestReport getTestReport(String algorithm, String reportID) throws GrillException;
  public Prediction predict(String algorithm, String modelID, Object[] features) throws GrillException;
  public void deleteModel(String algorithm, String modelID) throws GrillException;
  public void deleteTestReport(String algorithm, String reportID) throws GrillException;
}
