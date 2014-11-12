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
package org.apache.lens.server.query;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hive.service.cli.CLIService;
import org.apache.lens.api.LensException;
import org.apache.lens.cube.parse.CubeQueryRewriter;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryRewriter;

/**
 * The Class RewriteUtil.
 */
public class RewriteUtil {
  public static final Log LOG = LogFactory.getLog(RewriteUtil.class);

  /** The cube pattern. */
  static Pattern cubePattern = Pattern.compile(".*CUBE(.*)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE
      | Pattern.DOTALL);

  /** The matcher. */
  static Matcher matcher = null;

  /**
   * The Class CubeQueryInfo.
   */
  static class CubeQueryInfo {

    /** The start pos. */
    int startPos;

    /** The end pos. */
    int endPos;

    /** The query. */
    String query;

    /** The cube ast. */
    ASTNode cubeAST;
  }

  /**
   * Find cube positions.
   *
   * @param query
   *          the query
   * @return the list
   * @throws SemanticException
   *           the semantic exception
   * @throws ParseException
   *           the parse exception
   */
  static List<CubeQueryInfo> findCubePositions(String query) throws SemanticException, ParseException {
    ASTNode ast = HQLParser.parseHQL(query);
    LOG.debug("User query AST:" + ast.dump());
    List<CubeQueryInfo> cubeQueries = new ArrayList<CubeQueryInfo>();
    findCubePositions(ast, cubeQueries, query);
    for (CubeQueryInfo cqi : cubeQueries) {
      cqi.query = query.substring(cqi.startPos, cqi.endPos);
    }
    return cubeQueries;
  }

  /**
   * Find cube positions.
   *
   * @param ast
   *          the ast
   * @param cubeQueries
   *          the cube queries
   * @param originalQuery
   *          the original query
   * @throws SemanticException
   *           the semantic exception
   */
  private static void findCubePositions(ASTNode ast, List<CubeQueryInfo> cubeQueries, String originalQuery)
      throws SemanticException {
    int child_count = ast.getChildCount();
    if (ast.getToken() != null) {
      if (ast.getChild(0) != null) {
        LOG.debug("First child:" + ast.getChild(0) + " Type:"
            + ((ASTNode) ast.getChild(0)).getToken().getType());
      }
      if (ast.getToken().getType() == HiveParser.TOK_QUERY
          && ((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        LOG.debug("Inside cube clause");
        CubeQueryInfo cqi = new CubeQueryInfo();
        cqi.cubeAST = ast;
        if (ast.getParent() != null) {
          ASTNode parent = (ASTNode) ast.getParent();
          cqi.startPos = ast.getCharPositionInLine();
          int ci = ast.getChildIndex();
          if (parent.getToken() == null || parent.getToken().getType() == HiveParser.TOK_EXPLAIN
              || parent.getToken().getType() == HiveParser.TOK_CREATETABLE) {
            // Not a sub query
            cqi.endPos = originalQuery.length();
          } else if (parent.getChildCount() > ci + 1) {
            if (parent.getToken().getType() == HiveParser.TOK_SUBQUERY) {
              // less for the next start and for close parenthesis
              cqi.endPos = getEndPos(originalQuery, parent.getChild(ci + 1).getCharPositionInLine(), ")");
              ;
            } else if (parent.getToken().getType() == HiveParser.TOK_UNION) {
              // one less for the next start and less the size of string 'UNION ALL'
              cqi.endPos = getEndPos(originalQuery, parent.getChild(ci + 1).getCharPositionInLine() - 1, "UNION ALL");
            } else {
              // Not expected to reach here
              LOG.warn("Unknown query pattern found with AST:" + ast.dump());
              throw new SemanticException("Unknown query pattern");
            }
          } else {
            // last child of union all query
            // one for next AST
            // and one for the close parenthesis if there are no more unionall
            // or one for the string 'UNION ALL' if there are more union all
            LOG.debug("Child of union all");
            cqi.endPos = getEndPos(originalQuery, parent.getParent().getChild(1).getCharPositionInLine(), ")",
                "UNION ALL");
          }
        }
        LOG.debug("Adding cqi " + cqi + " query:" + originalQuery.substring(cqi.startPos, cqi.endPos));
        cubeQueries.add(cqi);
      } else {
        for (int child_pos = 0; child_pos < child_count; ++child_pos) {
          findCubePositions((ASTNode) ast.getChild(child_pos), cubeQueries, originalQuery);
        }
      }
    } else {
      LOG.warn("Null AST!");
    }
  }

  /**
   * Gets the end pos.
   *
   * @param query
   *          the query
   * @param backTrackIndex
   *          the back track index
   * @param backTrackStr
   *          the back track str
   * @return the end pos
   */
  private static int getEndPos(String query, int backTrackIndex, String... backTrackStr) {
    if (backTrackStr != null) {
      String q = query.substring(0, backTrackIndex).toLowerCase();
      for (int i = 0; i < backTrackStr.length; i++) {
        if (q.trim().endsWith(backTrackStr[i].toLowerCase())) {
          backTrackIndex = q.lastIndexOf(backTrackStr[i].toLowerCase());
          break;
        }
      }
    }
    while (Character.isSpaceChar(query.charAt(backTrackIndex - 1))) {
      backTrackIndex--;
    }
    return backTrackIndex;
  }

  /**
   * Gets the rewriter.
   *
   * @param driver
   *          the driver
   * @param queryConf
   *          the query conf
   * @return the rewriter
   * @throws SemanticException
   *           the semantic exception
   */
  static CubeQueryRewriter getRewriter(LensDriver driver, Configuration queryConf) throws SemanticException {
    return new CubeQueryRewriter(CubeQueryRewriter.getFinalQueryConf(driver, queryConf));
  }

  /**
   * Replaces new lines with spaces; '&&' with AND; '||' with OR // these two can be removed once HIVE-5326 gets
   * resolved.
   *
   * @param query
   *          the query
   * @return the replaced query
   */
  static String getReplacedQuery(final String query) {
    String finalQuery = query.replaceAll("[\\n\\r]", " ").replaceAll("&&", " AND ").replaceAll("\\|\\|", " OR ");
    return finalQuery;
  }

  public static String rewriteToCubeQL(final String query,  Configuration queryconf) throws LensException {
    Collection<QueryRewriter> queryRewriters = RewriteUtil.getQueryRewriter(queryconf);
    String rewrittenQuery = query;
    for (QueryRewriter rewriter : queryRewriters) {
      rewrittenQuery = rewriter.rewrite(rewrittenQuery, queryconf);
      if (StringUtils.isBlank(rewrittenQuery)) {
        throw new LensException("Query rewrite failed for " + query);
      }
    }
    return rewrittenQuery;
  }
  /**
   * Rewrite query.
   *
   * @param query
   *          the query
   * @param drivers
   *          the drivers
   * @param queryconf
   *          the queryconf
   * @return the map
   * @throws LensException
   *           the lens exception
   */
  public static Map<LensDriver, String> rewriteToHQL(final String query, Collection<LensDriver> drivers,
      Configuration queryconf) throws LensException {
    try {
      String replacedQuery = getReplacedQuery(query);
      String lowerCaseQuery = replacedQuery.toLowerCase();
      Map<LensDriver, String> driverQueries = new HashMap<LensDriver, String>();
      StringBuilder rewriteFailure = new StringBuilder();
      String failureCause = null;
      boolean useBuilder = false;
      if (lowerCaseQuery.startsWith("add") || lowerCaseQuery.startsWith("set")) {
        for (LensDriver driver : drivers) {
          driverQueries.put(driver, replacedQuery);
        }
      } else {
        List<RewriteUtil.CubeQueryInfo> cubeQueries = findCubePositions(replacedQuery);
        for (LensDriver driver : drivers) {
          CubeQueryRewriter rewriter = getRewriter(driver, queryconf);
          StringBuilder builder = new StringBuilder();
          int start = 0;
          try {
            for (RewriteUtil.CubeQueryInfo cqi : cubeQueries) {
              LOG.debug("Rewriting cube query:" + cqi.query);
              if (start != cqi.startPos) {
                builder.append(replacedQuery.substring(start, cqi.startPos));
              }
              String hqlQuery = rewriter.rewrite(cqi.query).toHQL();
              LOG.debug("Rewritten query:" + hqlQuery);
              builder.append(hqlQuery);
              start = cqi.endPos;
            }
            builder.append(replacedQuery.substring(start));
            String finalQuery = builder.toString();
            LOG.info("Final rewritten query for driver:" + driver + " is: " + finalQuery);
            driverQueries.put(driver, finalQuery);
          } catch (SemanticException e) {
            LOG.warn("Driver : " + driver.getClass().getName() + " Skipped for the query rewriting due to "
                + e.getMessage());
            rewriteFailure.append(" Driver :").append(driver.getClass().getName());
            rewriteFailure.append(" Cause :" + e.getLocalizedMessage());
            if (failureCause != null && !failureCause.equals(e.getLocalizedMessage())) {
              useBuilder = true;
            }
            if (failureCause == null) {
              failureCause = e.getLocalizedMessage();
            }
          }
        }
      }
      if (driverQueries.isEmpty()) {
        throw new LensException("No driver accepted the query, because "
            + (useBuilder ? rewriteFailure.toString() : failureCause));
      }
      return driverQueries;
    } catch (Exception e) {
      throw new LensException("Rewriting failed, cause :" + e.getMessage(), e);
    }
  }

  /**
   * Checks if is cube query.
   *
   * @param query
   *          the query
   * @return true, if is cube query
   */
  public static boolean isCubeQuery(String query) {
    if (matcher == null) {
      matcher = cubePattern.matcher(query);
    } else {
      matcher.reset(query);
    }
    return matcher.matches();
  }

  /**
   * Returns the rewriter chain implementations in the specified order in configuration.
   *
   * @param conf
   *          the query conf
   * @return the rewriter chain
   *         empty list if no rewriters configured
   */
  public static Collection<QueryRewriter> getQueryRewriter(Configuration conf) {
    String[] rewriterNames = conf.getStrings(LensConfConstants.QUERY_REWRITERS);
    if(rewriterNames == null || rewriterNames.length == 0) {
      return Collections.emptyList();
    }
    List<QueryRewriter> rewriterInsts = new ArrayList<QueryRewriter>(rewriterNames.length);
    for (String rName : rewriterNames) {
      QueryRewriter rewriterInst = loadRewriterImpl(rName, conf, Thread.currentThread().getContextClassLoader());
      if(rewriterInst != null) {
        rewriterInsts.add(rewriterInst);
      }
    }
    return rewriterInsts;
  }


  /**
   * Returns the rewriter chain implementations in the specified order in configuration and loads them through the
   * specified classloader.
   * @param conf
   *          the query conf
   * @param classLoader
   * @return
   */

  public static Collection<QueryRewriter> getQueryRewriter(Configuration conf, ClassLoader classLoader) {
    String[] rewriterNames = conf.getStrings(LensConfConstants.QUERY_REWRITERS);
    if(rewriterNames == null || rewriterNames.length == 0) {
      return Collections.emptyList();
    }
    List<QueryRewriter> rewriterInsts = new ArrayList<QueryRewriter>(rewriterNames.length);
    for (String rName : rewriterNames) {
      QueryRewriter rewriterInst = loadRewriterImpl(rName, conf, classLoader);
      if(rewriterInst != null) {
        rewriterInsts.add(rewriterInst);
      }
    }
    return rewriterInsts;
  }

  /**
   * Loads the specified class for a rewriter
   * @param rewriterName
   * @param conf
   * @param classLoader
   * @return
   */
  private static QueryRewriter loadRewriterImpl(String rewriterName, Configuration conf, ClassLoader classLoader) {
    try {
      String rewriterClassName = conf.get(LensConfConstants.getRewriterImplConfKey(rewriterName));

      if (StringUtils.isBlank(rewriterClassName)) {
        LOG.warn("Invalid class configured for rewriter " + rewriterName);
        return null;
      }

      Class<?> cls = classLoader.loadClass(rewriterClassName);

      if (QueryRewriter.class.isAssignableFrom(cls)) {
        Class<? extends QueryRewriter> rewriterClass = (Class<? extends QueryRewriter>) cls;
        LOG.info("Adding " + rewriterName + " service with " + rewriterClass);
        Constructor<?> constructor = rewriterClass.getConstructor();
        return (QueryRewriter) constructor.newInstance();
      } else {
        LOG.warn("Unsupported rewriter class " + rewriterClassName + " for rewriter " + rewriterName);
      }
    } catch (Exception e) {
      LOG.warn("Could not add rewriter:" + rewriterName, e);
      throw new RuntimeException("Could not add rewriter:" + rewriterName, e);
    }
    return null;
  }

}
