/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.query;

<<<<<<< HEAD
import lombok.Getter;
import lombok.Setter;
=======
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
import java.util.*;

import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryCost;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.util.LensUtil;
<<<<<<< HEAD

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
=======

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import lombok.Getter;
import lombok.Setter;
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

public class DriverSelectorQueryContext {

  /**
   * The constant LOG
   */
  public static final Log LOG = LogFactory.getLog(DriverSelectorQueryContext.class);


  /**
   * The selected driver.
   */
  @Getter
  @Setter
  protected LensDriver selectedDriver;

  /**
   * Map of driver to driver specific query context
   */
  @Getter
  @Setter
  protected Map<LensDriver, DriverQueryContext> driverQueryContextMap = new HashMap<LensDriver,
  DriverQueryContext>();

  public DriverSelectorQueryContext(final String userQuery, final Configuration queryConf,
      final Collection<LensDriver> drivers) {
    for (LensDriver driver : drivers) {
      DriverQueryContext ctx = new DriverQueryContext(driver);
      ctx.setDriverSpecificConf(mergeConf(driver, queryConf));
      ctx.setQuery(userQuery);
      driverQueryContextMap.put(driver, ctx);
    }
  }

  public static class DriverQueryContext {
<<<<<<< HEAD

    @Getter
    protected LensDriver driver;

    DriverQueryContext(LensDriver driver) {
      this.driver = driver;
    }

    /**
     * Map of driver to query plan
=======

    @Getter
    protected LensDriver driver;

    DriverQueryContext(LensDriver driver) {
      this.driver = driver;
    }

    /**
     * Driver's query cost
     */
    @Getter
    @Setter
    private QueryCost driverCost;

    /**
     * Driver's query plan
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
     */
    @Getter
    @Setter
    protected DriverQueryPlan driverQueryPlan;

    /**
<<<<<<< HEAD
     * driver specific query conf
=======
     * Driver specific query conf
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
     */
    @Getter
    @Setter
    protected Configuration driverSpecificConf;

    @Getter
    @Setter
<<<<<<< HEAD
=======
    /** exceptions occurred while trying to estimate cost */
    protected Exception driverQueryCostEstimateError;

    @Getter
    @Setter
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    /** exceptions occurred while trying to generate plans by explain call */
    protected Exception driverQueryPlanGenerationError;

    @Getter
    @Setter
    /** driver query */
    protected String query;

  }

  /**
   * Gets the driver query conf.
   *
   * @param driver    the driver
   * @param queryConf the query conf
   * @return the final query conf
   */
  private Configuration mergeConf(LensDriver driver, Configuration queryConf) {
    Configuration conf = new Configuration(driver.getConf());
    for (Map.Entry<String, String> entry : queryConf) {
      conf.set(entry.getKey(), entry.getValue());
    }
    conf.setClassLoader(queryConf.getClassLoader());
    return conf;
  }

  /**
   * Sets driver queries, generates plans for each driver by calling explain with respective queries,
   * Sets driverQueryPlans
   *
   * @param driverQueries
   * @throws LensException
   */
<<<<<<< HEAD
  void setDriverQueriesAndPlans(Map<LensDriver, String> driverQueries, AbstractQueryContext qctx)
    throws LensException {
    StringBuilder detailedFailureCause = new StringBuilder();
    String failureCause = null;
    boolean useBuilder = false;
    boolean succeededOnAtleastOneDriver = false;
    for (LensDriver driver : driverQueries.keySet()) {
      final DriverQueryContext driverQueryContext = driverQueryContextMap.get(driver);
      driverQueryContext.setQuery(driverQueries.get(driver));
=======
  void setDriverQueries(Map<LensDriver, String> driverQueries) {
    for (LensDriver driver : driverQueries.keySet()) {
      final DriverQueryContext driverQueryContext = driverQueryContextMap.get(driver);
      driverQueryContext.setQuery(driverQueries.get(driver));
    }
  }

  /**
   * Sets driver queries, generates plans for each driver by calling explain with respective queries,
   * Sets driverQueryPlans
   *
   * @param driverQueries
   * @throws LensException
   */
  public void setDriverQueryPlans(Map<LensDriver, String> driverQueries, AbstractQueryContext qctx)
    throws LensException {
    StringBuilder detailedFailureCause = new StringBuilder();
    String failureCause = null;
    boolean useBuilder = false;
    boolean succeededOnAtleastOneDriver = false;
    for (LensDriver driver : driverQueries.keySet()) {
      final DriverQueryContext driverQueryContext = driverQueryContextMap.get(driver);
      driverQueryContext.setQuery(driverQueries.get(driver));
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
      try {
        driverQueryContext.setDriverQueryPlan(driver.explain(qctx));
        succeededOnAtleastOneDriver = true;
      } catch (Exception e) {
        LOG.error("Setting driver plan failed for driver " + driver, e);
        String expMsg = LensUtil.getCauseMessage(e);
        driverQueryContext.setDriverQueryPlanGenerationError(e);
        detailedFailureCause.append("\n Driver :").append(driver.getClass().getName());
        detailedFailureCause.append(" Cause :" + expMsg);
        if (failureCause != null && !failureCause.equals(expMsg)) {
          useBuilder = true;
        }
        if (failureCause == null) {
          failureCause = expMsg;
        }
      }
    }
    if (!succeededOnAtleastOneDriver) {
      throw new LensException(useBuilder ? detailedFailureCause.toString() : failureCause);
    }
  }

  /**
   * Return selected driver's query plan, but check for null conditions first.
   *
   * @return DriverQueryPlan of Selected Driver
   * @throws LensException
   */
  public DriverQueryPlan getSelectedDriverQueryPlan() throws LensException {
    final Map<LensDriver, DriverQueryContext> driverQueryCtxs = getDriverQueryContextMap();
    if (driverQueryCtxs == null) {
      throw new LensException("No Driver query ctx. Check if re-write happened or not");
    }
    if (getSelectedDriver() == null) {
      throw new LensException("Selected Driver is NULL.");
    }

    if (driverQueryCtxs.get(getSelectedDriver()) == null) {
      throw new LensException("Could not find Driver Context for selected driver " + getSelectedDriver());
    }

    if (driverQueryCtxs.get(getSelectedDriver()).getDriverQueryPlanGenerationError() != null) {
      throw new LensException("Driver Query Plan of the selected driver is null",
          driverQueryCtxs.get(getSelectedDriver()).getDriverQueryPlanGenerationError());
<<<<<<< HEAD
    }
    return driverQueryCtxs.get(getSelectedDriver()).getDriverQueryPlan();
  }

  public Configuration getSelectedDriverConf() {
    return getSelectedDriver() == null ? null : driverQueryContextMap.get(getSelectedDriver()).getDriverSpecificConf();
  }

  public String getSelectedDriverQuery() {
    return getSelectedDriver() == null ? null : driverQueryContextMap.get(getSelectedDriver()).getQuery();
  }

  public void setDriverConf(LensDriver driver, Configuration conf) {
    driverQueryContextMap.get(driver).setDriverSpecificConf(conf);
  }

  public void setSelectedDriverQuery(String driverQuery) {
    if (driverQueryContextMap != null && driverQueryContextMap.get(getSelectedDriver()) != null) {
      driverQueryContextMap.get(getSelectedDriver()).setQuery(driverQuery);
    }
  }

  public Collection<LensDriver> getDrivers() {
    return driverQueryContextMap.keySet();
  }

  public Collection<String> getDriverQueries() {
    List<String> queries = new ArrayList<String>();
    final Collection<DriverQueryContext> values = driverQueryContextMap.values();
    for (DriverQueryContext ctx : values) {
      if (ctx.getQuery() != null) {
        queries.add(ctx.getQuery());
      }
    }
=======
    }
    return driverQueryCtxs.get(getSelectedDriver()).getDriverQueryPlan();
  }

  /**
   * Return selected driver's query plan, but check for null conditions first.
   *
   * @return DriverQueryPlan of Selected Driver
   * @throws LensException
   */
  public QueryCost getSelectedDriverQueryCost() throws LensException {
    final Map<LensDriver, DriverQueryContext> driverQueryCtxs = getDriverQueryContextMap();
    if (driverQueryCtxs == null) {
      throw new LensException("No Driver query ctx. Check if re-write happened or not");
    }
    if (getSelectedDriver() == null) {
      throw new LensException("Selected Driver is NULL.");
    }

    if (driverQueryCtxs.get(getSelectedDriver()) == null) {
      throw new LensException("Could not find Driver Context for selected driver " + getSelectedDriver());
    }

    if (driverQueryCtxs.get(getSelectedDriver()).getDriverQueryCostEstimateError() != null) {
      throw new LensException("Driver Query Cost of the selected driver is null",
          driverQueryCtxs.get(getSelectedDriver()).getDriverQueryCostEstimateError());
    }
    return driverQueryCtxs.get(getSelectedDriver()).getDriverCost();
  }

  public Configuration getSelectedDriverConf() {
    return getSelectedDriver() == null ? null : driverQueryContextMap.get(getSelectedDriver()).getDriverSpecificConf();
  }

  public String getSelectedDriverQuery() {
    return getSelectedDriver() == null ? null : driverQueryContextMap.get(getSelectedDriver()).getQuery();
  }

  public void setDriverConf(LensDriver driver, Configuration conf) {
    driverQueryContextMap.get(driver).setDriverSpecificConf(conf);
  }

  public void setSelectedDriverQuery(String driverQuery) {
    if (driverQueryContextMap != null && driverQueryContextMap.get(getSelectedDriver()) != null) {
      driverQueryContextMap.get(getSelectedDriver()).setQuery(driverQuery);
    }
  }

  public Collection<LensDriver> getDrivers() {
    return driverQueryContextMap.keySet();
  }

  public Collection<String> getDriverQueries() {
    List<String> queries = new ArrayList<String>();
    final Collection<DriverQueryContext> values = driverQueryContextMap.values();
    for (DriverQueryContext ctx : values) {
      if (ctx.getQuery() != null) {
        queries.add(ctx.getQuery());
      }
    }
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    return queries;
  }

  public DriverQueryPlan getDriverQueryPlan(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
        ? driverQueryContextMap.get(driver).getDriverQueryPlan() : null;
  }

  public Configuration getDriverConf(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
        ? driverQueryContextMap.get(driver).getDriverSpecificConf() : null;
  }

  public String getDriverQuery(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
        ? driverQueryContextMap.get(driver).getQuery() : null;
<<<<<<< HEAD
=======
  }

  public QueryCost getDriverQueryCost(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
        ? driverQueryContextMap.get(driver).getDriverCost() : null;
  }

  public void setDriverQueryPlan(LensDriver driver, DriverQueryPlan qp) {
    driverQueryContextMap.get(driver).setDriverQueryPlan(qp);
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
  }
}
