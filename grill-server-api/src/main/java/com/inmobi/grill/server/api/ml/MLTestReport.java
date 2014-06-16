package com.inmobi.grill.server.api.ml;

import java.util.List;

public interface MLTestReport {
  public String getReportID();
  public String getModelID();
  public String getTestOutputPath();
  public String getLabelColumn();
  public String getPredictionResultColumn();
  public String getAlgorithm();
  public List<String> getFeatureColumns();
  public String getGrillQueryID();
  public List<MLTestMetric> getMetrics();
  public MLTestMetric getMetric(String metricName);
}
