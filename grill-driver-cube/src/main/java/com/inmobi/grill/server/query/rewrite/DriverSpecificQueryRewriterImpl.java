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

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.driver.cube.CubeGrillDriver;
import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.query.*;
import com.inmobi.grill.server.api.query.rewrite.CubeQLCommand;
import com.inmobi.grill.server.api.query.rewrite.HQLCommand;
import com.inmobi.grill.server.api.query.rewrite.NonSQLCommand;
import com.inmobi.grill.server.api.query.rewrite.QueryCommand;
import com.inmobi.grill.server.api.query.rewrite.dsl.DSLCommand;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import parquet.Preconditions;
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

  public DriverSpecificQueryRewriterImpl(QueryContext ctx) {
    this.ctx = ctx;
  }

  public DriverSpecificQueryRewriterImpl(PreparedQueryContext ctx) {
    this.prepCtx = ctx;
  }

  public DriverSpecificQueryRewriterImpl() {
  }

  /**
   *
   * @param queryCmd The query which needs to be rewritten
   * @param drivers Grill drivers for which the query needs to be rewritten
   * @return
   * @throws GrillException
   */
  @Override
  public Map<GrillDriver, HQLCommand> rewrite(QueryCommand queryCmd, Collection<GrillDriver> drivers) throws GrillException {

    final QueryCommand.Type type = queryCmd.getType();
    Preconditions.checkNotNull(type, "Unable to parse Query Command " + queryCmd.getCommand());
    if(QueryCommand.Type.NONSQL.equals(type)) {
      doNonSQLRewrites((NonSQLCommand)queryCmd, drivers);
    }

    //Rewrite to CubeQL if needed
    QueryCommand rewrittenQL = null;
    if (QueryCommand.Type.DOMAIN.equals(type)) {
      DSLCommand dslCommand = (DSLCommand) queryCmd;
      rewrittenQL = dslCommand.rewrite();
      Preconditions.checkArgument(rewrittenQL.getType() != QueryCommand.Type.DOMAIN, "DSL query cannot be rewritten to DSL");
      return rewrite(rewrittenQL, drivers);
    } else if (QueryCommand.Type.CUBE.equals(type)) {
      CubeQLCommand cubeQL = (CubeQLCommand) queryCmd;
      return doCubeRewrites(cubeQL, drivers);
    }
    return null;
  }

  private  Map<GrillDriver, HQLCommand> doNonSQLRewrites(NonSQLCommand cmd, Collection<GrillDriver> drivers) throws GrillException {
    Map<GrillDriver, HQLCommand> driverSpecificHQLs = new HashMap<GrillDriver, HQLCommand>();
    for(GrillDriver driver : drivers) {
      final HQLCommand hqlCommand = cmd.rewrite();
      driverSpecificHQLs.put(driver, hqlCommand);
    }
    return driverSpecificHQLs;
  }

  private Map<GrillDriver, HQLCommand> doCubeRewrites(CubeQLCommand cubeQL, Collection<GrillDriver> drivers) throws GrillException {
    Map<GrillDriver, HQLCommand> driverSpecificHQLs = new HashMap<GrillDriver, HQLCommand>();
    try {
      //Validate if rewritten query can be parsed
      cubeQL.parse();
    } catch (SemanticException e) {
      throw new GrillException(e);
    } catch (ParseException e) {
      throw new GrillException(e);
    }

    //Update query context with the cubeQL
    updateQueryContext(cubeQL);

    //Rewrite to driver specific HQL queries
    DriverSpecificRewriteException rewriteError = new DriverSpecificRewriteException("No driver accepted the query, because ");
    for (GrillDriver driver : drivers) {
      try {
        //Set Driver specific Query Conf
        cubeQL.setConf(CubeQLCommandImpl.getDriverQueryConf(driver, cubeQL.getConf()));
        final HQLCommand driverSpecificHQL = cubeQL.rewrite();
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


  private void updateQueryContext(CubeQLCommand queryCmd) throws GrillException {
    //Update cube query in context
    if(prepCtx != null) {
      prepCtx.setCubeQuery(queryCmd.getCommand());
    } else if(ctx != null) {
      ctx.setCubeQuery(queryCmd.getCommand());
    }
  }
}
