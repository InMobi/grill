package com.inmobi.grill.ml;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.QueryHandle;

/**
 * Run the model testing query against a grill server
 */
public abstract class TestQueryRunner {
  protected final GrillSessionHandle sessionHandle;

  public TestQueryRunner(GrillSessionHandle sessionHandle) {
    this.sessionHandle = sessionHandle;
  }

  public abstract QueryHandle runQuery(String query) throws GrillException;
}
