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
package org.apache.lens.server.query.rewrite;

import java.util.Collection;
import java.util.Map;

import org.apache.lens.api.LensException;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.rewrite.QueryCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.lens.server.api.driver.LensDriver;

/**
 * The Class RewriteUtil.
 */
public class RewriteUtil {

  /**
   * Rewrite query.
   *
   * @param  ctx
   *          the query context
   * @param drivers
   *          the drivers
   * @return the map of trwritten queries for each driver
   * @throws LensException
   *           the lens exception
   */
  public static Map<LensDriver, QueryCommand> rewriteQuery(QueryContext ctx, Collection<LensDriver> drivers) throws LensException {
    DriverSpecificQueryRewrite rewriter = getQueryRewriter(ctx.getConf());
    rewriter.init(ctx);
    return rewrite(rewriter, ctx.getUserQuery(), ctx.getSubmittedUser(), ctx.getConf(), drivers);
  }

  /**
   * Rewrite query.
   *
   * @param  ctx
   *          the prepared query context
   * @param drivers
   *          the drivers
   * @return the map of trwritten queries for each driver
   * @throws LensException
   *           the lens exception
   */
  public static Map<LensDriver, QueryCommand> rewriteQuery(PreparedQueryContext ctx, Collection<LensDriver> drivers) throws LensException {
    DriverSpecificQueryRewrite rewriter = getQueryRewriter(ctx.getConf());
    rewriter.init(ctx);
    return rewrite(rewriter, ctx.getUserQuery(), ctx.getPreparedUser(), ctx.getConf(), drivers);
  }

  /**
   * Rewrite query.
   *
   * @param  query
   *          the query
   * @param  userName
   *          the user name
   * @param drivers
   *          the drivers
   * @return the map of trwritten queries for each driver
   * @throws LensException
   *           the lens exception
   */
  public static  Map<LensDriver, QueryCommand> rewriteQuery(String query, String userName, Configuration conf, Collection<LensDriver> drivers) throws LensException {
    DriverSpecificQueryRewrite rewriter = getQueryRewriter(conf);
    return rewrite(rewriter, query, userName, conf, drivers);
  }

  static Map<LensDriver, QueryCommand> rewrite(DriverSpecificQueryRewrite rewriter, String q1, String userName, Configuration conf, Collection<LensDriver> drivers) throws LensException {
    final QueryCommand queryCommand = QueryCommands.get(q1, userName, conf);
    return rewriter.rewrite(queryCommand, drivers);
  }

  /**
   * Gets the rewriter.
   *
   * @param conf
   *          the query conf
   * @return the rewriter
   * @throws org.apache.hadoop.hive.ql.parse.SemanticException
   *           the semantic exception
   */
  private static DriverSpecificQueryRewrite getQueryRewriter(Configuration conf) {
    Class<?> rewriterClass = conf.getClass(LensConfConstants.QUERY_REWRITER, null);
    try {
      return (DriverSpecificQueryRewrite) rewriterClass.newInstance();
    } catch (InstantiationException e) {
      throw new IllegalStateException("Could not load query rewriter class " + rewriterClass, e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Could not load query rewriter class " + rewriterClass, e);
    }
  }

  /**
   *
   * For tests only - Could not get it working with PowerMock without passing in the classloader
   *
   */

  static DriverSpecificQueryRewrite getQueryRewriter(Configuration conf, ClassLoader classLoader) {
    Class<?> rewriterClass = null;
    try {
      rewriterClass = classLoader.loadClass(conf.get(LensConfConstants.QUERY_REWRITER, null));
      return (DriverSpecificQueryRewrite) rewriterClass.newInstance();
    } catch (InstantiationException e) {
      throw new IllegalStateException("Could not load query rewriter class " + rewriterClass, e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Could not load query rewriter class " + rewriterClass, e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not load query rewriter class " + rewriterClass, e);
    }
  }
}
