package com.inmobi.grill.server.api.ml;

import java.util.List;

public interface RankingPrediction {
  public static class RankedElement {
    final String label;
    final double score;

    public RankedElement(String label, double score) {
      this.label = label;
      this.score = score;
    }

    public String getLabel() {
      return label;
    }

    public double getScore() {
      return score;
    }
  }

  public List<RankedElement> getPredictions();
}
