package com.inmobi.grill.server.ml;

import com.inmobi.grill.server.api.ml.MLTestMetric;
import com.inmobi.grill.server.api.ml.MLTestReport;

import java.util.List;

public class BaseTestReport implements MLTestReport {
  private String testTable;
  private String outputTable;
  private String outputColumn;
  private String labelColumn;
  private List<String> featureColumns;
  private String algorithm;
  private String modelID;
  private String reportID;
  private String queryID;

  @Override
  public String getReportID() {
    return reportID;
  }

  @Override
  public String getModelID() {
    return modelID;
  }

  @Override
  public String getTestOutputPath() {
    return testTable;
  }

  @Override
  public String getLabelColumn() {
    return labelColumn;
  }

  @Override
  public String getPredictionResultColumn() {
    return outputColumn;
  }

  @Override
  public String getAlgorithm() {
    return algorithm;
  }

  @Override
  public List<String> getFeatureColumns() {
    return featureColumns;
  }

  @Override
  public List<MLTestMetric> getMetrics() {
    throw new UnsupportedOperationException("Error metric not yet supported");
  }

  @Override
  public MLTestMetric getMetric(String metricName) {
    throw new UnsupportedOperationException("Error metric not yet supported");
  }

  @Override
  public String getGrillQueryID() {
    return queryID;
  }

  void setOutputColumn(String outputColumn) {
    this.outputColumn = outputColumn;
  }

  void setTestTable(String testTable) {
    this.testTable = testTable;
  }

  void setOutputTable(String outputTable) {
    this.outputTable = outputTable;
  }

  void setLabelColumn(String labelColumn) {
    this.labelColumn = labelColumn;
  }

  void setFeatureColumns(List<String> featureColumns) {
    this.featureColumns = featureColumns;
  }

  void setAlgorithm(String algorithm) {
    this.algorithm = algorithm;
  }

  void setModelID(String modelID) {
    this.modelID = modelID;
  }

  void setReportID(String reportID) {
    this.reportID = reportID;
  }
  void setQueryID(String queryID) {
    this.queryID = queryID;
  }
}
