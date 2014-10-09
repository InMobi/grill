package com.inmobi.grill.ml;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;

import java.util.List;
import java.util.Map;

/**
 * Grill's machine learning interface used by client code as well as Grill ML service.
 */
public interface GrillML {
  public static final String NAME = "ml";

  /**
   * Get list of available machine learning algorithms
   * @return
   */
  public List<String> getAlgorithms();

  /**
   * Get user friendly information about parameters accepted by the algorithm
   * @param algorithm
   * @return map of param key to its help message
   */
  public Map<String, String> getAlgoParamDescription(String algorithm);

  /**
   * Get a trainer object instance which could be used to generate a model of the given algorithm
   * @param algorithm
   * @return
   * @throws GrillException
   */
  public MLTrainer getTrainerForName(String algorithm) throws GrillException;

  /**
   * Create a model using the given HCatalog table as input. The arguments should contain information
   * needeed to generate the model.
   * @param table
   * @param algorithm
   * @param args
   * @return Unique ID of the model created after training is complete
   * @throws GrillException
   */
  public String train(String table, String algorithm, String[] args) throws GrillException;

  /**
   * Get model IDs for the given algorithm
   * @param algorithm
   * @return
   * @throws GrillException
   */
  public List<String> getModels(String algorithm) throws GrillException;

  /**
   * Get a model instance given the algorithm name and model ID
   * @param algorithm
   * @param modelId
   * @return
   * @throws GrillException
   */
  public MLModel getModel(String algorithm, String modelId) throws GrillException;

  /**
   * Get the FS location where model instance is saved
   * @param algorithm
   * @param modelID
   * @return
   */
  String getModelPath(String algorithm, String modelID);

  /**
   * Evaluate model by running it against test data contained in the given table
   * @param session
   * @param table
   * @param algorithm
   * @param modelID
   * @return Test report object containing test output table, and various evaluation metrics
   * @throws GrillException
   */
  public MLTestReport testModel(GrillSessionHandle session, String table, String algorithm, String modelID)
    throws GrillException;

  /**
   * Get test reports for an algorithm
   * @param algorithm
   * @return
   * @throws GrillException
   */
  public List<String> getTestReports(String algorithm) throws GrillException;

  /**
   * Get a test report by ID
   * @param algorithm
   * @param reportID
   * @return
   * @throws GrillException
   */
  public MLTestReport getTestReport(String algorithm, String reportID) throws GrillException;

  /**
   * Online predict call given a model ID, algorithm name and sample feature values
   * @param algorithm
   * @param modelID
   * @param features
   * @return prediction result
   * @throws GrillException
   */
  public Object predict(String algorithm, String modelID, Object[] features) throws GrillException;

  /**
   * Permanently delete a model instance
   * @param algorithm
   * @param modelID
   * @throws GrillException
   */
  public void deleteModel(String algorithm, String modelID) throws GrillException;

  /**
   * Permanently delete a test report instance
   * @param algorithm
   * @param reportID
   * @throws GrillException
   */
  public void deleteTestReport(String algorithm, String reportID) throws GrillException;
}

