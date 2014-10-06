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

import java.util.Collection;
import java.util.Map;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.query.*;
import com.inmobi.grill.server.api.query.rewrite.HQLCommand;
import com.inmobi.grill.server.api.query.rewrite.QueryCommand;
import org.apache.hadoop.conf.Configuration;
import com.inmobi.grill.server.api.driver.GrillDriver;

public class RewriteUtil {

  public static Map<GrillDriver, QueryCommand> rewriteQuery(QueryContext ctx, Collection<GrillDriver> drivers) throws GrillException {
    DriverSpecificQueryRewrite rewriter = getQueryRewriter(ctx.getConf());
    rewriter.init(ctx);
    return rewrite(rewriter, ctx.getUserQuery(), ctx.getSubmittedUser(), ctx.getConf(), drivers);
  }

  public static Map<GrillDriver, QueryCommand> rewriteQuery(PreparedQueryContext ctx, Collection<GrillDriver> drivers) throws GrillException {
    DriverSpecificQueryRewrite rewriter = getQueryRewriter(ctx.getConf());
    rewriter.init(ctx);
    return rewrite(rewriter, ctx.getUserQuery(), ctx.getPreparedUser(), ctx.getConf(), drivers);
  }

  public static  Map<GrillDriver, QueryCommand> rewriteQuery(String query, String userName, Configuration conf, Collection<GrillDriver> drivers) throws GrillException {
    DriverSpecificQueryRewrite rewriter = getQueryRewriter(conf);
    return rewrite(rewriter, query, userName, conf, drivers);
  }

  static Map<GrillDriver, QueryCommand> rewrite(DriverSpecificQueryRewrite rewriter, String q1, String userName, Configuration conf, Collection<GrillDriver> drivers) throws GrillException {
    final QueryCommand queryCommand = QueryCommands.get(q1, userName, conf);
    return rewriter.rewrite(queryCommand, drivers);
  }

  private static DriverSpecificQueryRewrite getQueryRewriter(Configuration conf) {
    Class<?> rewriterClass = conf.getClass(GrillConfConstants.GRILL_QUERY_REWRITER, null);
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
      rewriterClass = classLoader.loadClass(conf.get(GrillConfConstants.GRILL_QUERY_REWRITER, null));
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
