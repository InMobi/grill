package com.inmobi.grill.server.query.rewrite;

/*
 * #%L
 * Grill Cube Driver
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Preconditions;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.driver.cube.CubeGrillDriver;
import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.query.*;
import com.inmobi.grill.server.api.query.rewrite.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Rewrites the given query to Driver Specific HQL
 */
public class DriverSpecificQueryRewriterImpl implements DriverSpecificQueryRewrite {

  public static final Log LOG = LogFactory.getLog(DriverSpecificQueryRewriterImpl.class);
  QueryContext ctx;
  PreparedQueryContext prepCtx;

  public void init(QueryContext ctx) {
    this.ctx = ctx;
  }

  public void init(PreparedQueryContext ctx) {
    this.prepCtx = ctx;
  }

  /**
   *
   * @param queryCmd The query which needs to be rewritten
   * @param drivers Grill drivers for which the query needs to be rewritten
   * @return
   * @throws GrillException
   */
  @Override
  public Map<GrillDriver, QueryCommand> rewrite(QueryCommand queryCmd, Collection<GrillDriver> drivers) throws GrillException {

    final QueryCommand.Type type = queryCmd.getType();
    Preconditions.checkNotNull(type, "Unable to parse Query Command " + queryCmd.getCommand());
    switch (type) {
      case NONSQL:
        return doNonSQLRewrites(queryCmd, drivers);
      case DOMAIN:
        //Rewrite to CubeQL/HQL
        final QueryCommand rewrittenQuery = queryCmd.rewrite();
        if( !QueryCommand.Type.DOMAIN.getNextValidStates().contains(rewrittenQuery.getType())) {
          throw new DriverSpecificRewriteException("Invalid rewritten query type " + rewrittenQuery.getType());
        }
        return rewrite(rewrittenQuery, drivers);
      case CUBE:
        return doCubeRewrites(queryCmd, drivers);
      case HQL:
        Map<GrillDriver, QueryCommand> driverSpecificHQLs = new HashMap<GrillDriver, QueryCommand>();
        for(GrillDriver driver : drivers) {
          driverSpecificHQLs.put(driver, queryCmd);
        }
        return driverSpecificHQLs;
    }
    throw new IllegalArgumentException("Unknown query command type " + queryCmd.getCommand());
  }

  private  Map<GrillDriver, QueryCommand> doNonSQLRewrites(QueryCommand cmd, Collection<GrillDriver> drivers) throws GrillException {
    Map<GrillDriver, QueryCommand> driverSpecificHQLs = new HashMap<GrillDriver, QueryCommand>();
    for(GrillDriver driver : drivers) {
      final QueryCommand hqlCommand = cmd.rewrite();
      if(! QueryCommand.Type.NONSQL.getNextValidStates().contains(hqlCommand.getType())) {
        throw new DriverSpecificRewriteException("Invalid rewritten query type " + hqlCommand.getType());
      }
      driverSpecificHQLs.put(driver, hqlCommand);
    }
    return driverSpecificHQLs;
  }

  private Map<GrillDriver, QueryCommand> doCubeRewrites(QueryCommand cubeQL, Collection<GrillDriver> drivers) throws GrillException {
    Map<GrillDriver, QueryCommand> driverSpecificHQLs = new HashMap<GrillDriver, QueryCommand>();
    try {
      //Validate if rewritten query can be parsed
      ((CubeQLCommand) cubeQL).parse();
    } catch (SemanticException e) {
      throw new DriverSpecificRewriteException("Could not rewrite cubeQL " , e);
    } catch (ParseException e) {
      throw new DriverSpecificRewriteException("Could not parse cubeQL " , e);
    }

    //Update query context with the cubeQL
    updateQueryContext(cubeQL);

    //Rewrite to driver specific HQL queries
    DriverSpecificRewriteException rewriteError = new DriverSpecificRewriteException("No driver accepted the query, because ");
    for (GrillDriver driver : drivers) {
      try {
        //Set Driver specific Query Conf
        cubeQL.setConf(CubeQLCommandImpl.getDriverQueryConf(driver, cubeQL.getConf()));
        final QueryCommand driverSpecificHQL = cubeQL.rewrite();
        if(! QueryCommand.Type.CUBE.getNextValidStates().contains(driverSpecificHQL.getType())) {
          throw new DriverSpecificRewriteException("Invalid rewritten query type " + driverSpecificHQL.getType());
        }
        driverSpecificHQLs.put(driver, driverSpecificHQL);
      } catch(GrillException e) {
        CubeGrillDriver.LOG.warn("Driver : " + driver.getClass().getName() +
            " Skipped for the query rewriting due to " + e.getMessage());
        rewriteError.addDriverError(driver, e.getLocalizedMessage());
      }
    }
    if (driverSpecificHQLs.isEmpty()) {
      throw rewriteError;
    }
    return driverSpecificHQLs;
  }


  private void updateQueryContext(QueryCommand queryCmd) throws GrillException {
    //Update cube query in context
    if(prepCtx != null) {
      prepCtx.setCubeQuery(queryCmd.getCommand());
    } else if(ctx != null) {
      ctx.setCubeQuery(queryCmd.getCommand());
    }
  }
}
