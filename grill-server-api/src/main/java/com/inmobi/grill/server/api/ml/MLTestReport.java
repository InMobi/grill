package com.inmobi.grill.server.api.ml;

import java.io.Serializable;
import java.util.List;

public interface MLTestReport extends Serializable {
  public String getReportID();
  public String getModelID();
  public String getTestOutputPath();
  public String getLabelColumn();
  public String getPredictionResultColumn();
  public String getTestTable();
  public String getAlgorithm();
  public List<String> getFeatureColumns();
  public String getGrillQueryID();
  public List<MLTestMetric> getMetrics();
  public MLTestMetric getMetric(String metricName);
}
