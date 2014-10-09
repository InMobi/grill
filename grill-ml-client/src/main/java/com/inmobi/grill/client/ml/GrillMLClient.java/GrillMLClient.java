package com.inmobi.grill.client.ml;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.ml.ModelMetadata;
import com.inmobi.grill.api.ml.TestReport;
import com.inmobi.grill.client.GrillConnection;
import com.inmobi.grill.client.GrillConnectionParams;
import com.inmobi.grill.client.GrillMLJerseyClient;
import com.inmobi.grill.ml.GrillML;
import com.inmobi.grill.ml.MLModel;
import com.inmobi.grill.ml.MLTestReport;
import com.inmobi.grill.ml.MLTrainer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class GrillMLClient implements GrillML {

  private GrillMLJerseyClient client;

  public GrillMLClient(GrillConnectionParams clientConf) {
    client = new GrillMLJerseyClient(new GrillConnection(clientConf));
  }

  /**
   * Get list of available machine learning algorithms
   *
   * @return
   */
  @Override
  public List<String> getAlgorithms() {
    return client.getTrainerNames();
  }

  /**
   * Get user friendly information about parameters accepted by the algorithm
   *
   * @param algorithm
   * @return map of param key to its help message
   */
  @Override
  public Map<String, String> getAlgoParamDescription(String algorithm) {
    List<String> paramDesc = client.getParamDescriptionOfTrainer(algorithm);
    // convert paramDesc to map
    Map<String, String> paramDescMap = new LinkedHashMap<String, String>();
    for (String str : paramDesc) {
      String[] keyHelp = StringUtils.split(str, ":");
      paramDescMap.put(keyHelp[0].trim(), keyHelp[1].trim());
    }
    return paramDescMap;
  }

  /**
   * Get a trainer object instance which could be used to generate a model of the given algorithm
   *
   * @param algorithm
   * @return
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public MLTrainer getTrainerForName(String algorithm) throws GrillException {
    throw new UnsupportedOperationException("MLTrainer cannot be accessed from client");
  }

  /**
   * Create a model using the given HCatalog table as input. The arguments should contain information
   * needeed to generate the model.
   *
   * @param table
   * @param algorithm
   * @param args
   * @return Unique ID of the model created after training is complete
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public String train(String table, String algorithm, String[] args) throws GrillException {
    Map<String, String> trainParams = new LinkedHashMap<String, String>();
    trainParams.put("table", table);
    for (int i = 0; i < args.length; i += 2) {
      trainParams.put(args[i], args[i+1]);
    }
    return client.trainModel(algorithm, trainParams);
  }

  /**
   * Get model IDs for the given algorithm
   *
   * @param algorithm
   * @return
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public List<String> getModels(String algorithm) throws GrillException {
    return client.getModelsForAlgorithm(algorithm);
  }

  /**
   * Get a model instance given the algorithm name and model ID
   *
   * @param algorithm
   * @param modelId
   * @return
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public MLModel getModel(String algorithm, String modelId) throws GrillException {
    ModelMetadata metadata = client.getModelMetadata(algorithm, modelId);
    String modelPathURI = metadata.getModelPath();

    ObjectInputStream in = null;
    try {
      URI modelURI = new URI(modelPathURI);
      Path modelPath = new Path(modelURI);
      FileSystem fs = FileSystem.get(modelURI, client.getConf());
      in = new ObjectInputStream(fs.open(modelPath));
      MLModel<?> model = (MLModel) in.readObject();
      return model;
    } catch (IOException e) {
      throw new GrillException(e);
    } catch (URISyntaxException e) {
      throw new GrillException(e);
    } catch (ClassNotFoundException e) {
      throw new GrillException(e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

  }

  /**
   * Get the FS location where model instance is saved
   *
   * @param algorithm
   * @param modelID
   * @return
   */
  @Override
  public String getModelPath(String algorithm, String modelID) {
    ModelMetadata metadata = client.getModelMetadata(algorithm, modelID);
    return metadata.getModelPath();
  }

  /**
   * Evaluate model by running it against test data contained in the given table
   *
   * @param session
   * @param table
   * @param algorithm
   * @param modelID
   * @return Test report object containing test output table, and various evaluation metrics
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public MLTestReport testModel(GrillSessionHandle session, String table, String algorithm, String modelID)
    throws GrillException {
    String reportID = client.testModel(table, algorithm, modelID);
    return getTestReport(algorithm, reportID);
  }

  /**
   * Get test reports for an algorithm
   *
   * @param algorithm
   * @return
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public List<String> getTestReports(String algorithm) throws GrillException {
    return client.getTestReportsOfAlgorithm(algorithm);
  }

  /**
   * Get a test report by ID
   *
   * @param algorithm
   * @param reportID
   * @return
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public MLTestReport getTestReport(String algorithm, String reportID) throws GrillException {
    TestReport report = client.getTestReport(algorithm, reportID);
    MLTestReport mlTestReport = new MLTestReport();
    mlTestReport.setAlgorithm(report.getAlgorithm());
    mlTestReport.setFeatureColumns(Arrays.asList(report.getFeatureColumns().split("\\,+")));
    mlTestReport.setGrillQueryID(report.getQueryID());
    mlTestReport.setLabelColumn(report.getLabelColumn());
    mlTestReport.setModelID(report.getModelID());
    mlTestReport.setOutputColumn(report.getOutputColumn());
    mlTestReport.setPredictionResultColumn(report.getOutputColumn());
    mlTestReport.setQueryID(report.getQueryID());
    mlTestReport.setReportID(report.getReportID());
    mlTestReport.setTestTable(report.getTestTable());
    return mlTestReport;
  }

  /**
   * Online predict call given a model ID, algorithm name and sample feature values
   *
   * @param algorithm
   * @param modelID
   * @param features
   * @return prediction result
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public Object predict(String algorithm, String modelID, Object[] features) throws GrillException {
    return getModel(algorithm, modelID).predict(features);
  }

  /**
   * Permanently delete a model instance
   *
   * @param algorithm
   * @param modelID
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public void deleteModel(String algorithm, String modelID) throws GrillException {
    client.deleteModel(algorithm, modelID);
  }

  /**
   * Permanently delete a test report instance
   *
   * @param algorithm
   * @param reportID
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public void deleteTestReport(String algorithm, String reportID) throws GrillException {
    client.deleteTestReport(algorithm, reportID);
  }
}
