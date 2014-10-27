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

import org.apache.lens.driver.cube.CubeDriver;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.rewrite.CubeQLCommand;
import org.apache.lens.server.api.query.rewrite.HQLCommand;
import org.apache.lens.server.api.query.rewrite.RewriteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryRewriter;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class CubeQLCommandImpl extends CubeQLCommand {

  public static final Log LOG = LogFactory.getLog(CubeQLCommandImpl.class);
  private List<CubeQueryInfo> cubeQueries = null;
  private boolean parsed = false;

  public CubeQLCommandImpl(String command, String userName, Configuration conf) {
    super(command, userName, conf);
  }

  public CubeQLCommandImpl() {
  }

  @Override
  public HQLCommand rewrite() throws RewriteException {
    HQLCommand hqlCommand = new HQLCommandImpl(this);
    String cubeQuery = getCommand();
    CubeQueryRewriter rewriter = getRewriter(conf) ;
    StringBuilder builder = new StringBuilder();
    int start = 0;
    try {
      if(!parsed) {
        parse();
      }
      for (CubeQueryInfo cqi : cubeQueries) {
        CubeDriver.LOG.debug("Rewriting cube query:" + cqi.query);
        if (start != cqi.startPos) {
          builder.append(cubeQuery.substring(start, cqi.startPos));
        }
        String hqlQuery = rewriter.rewrite(cqi.query).toHQL();
        CubeDriver.LOG.debug("Rewritten query:" + hqlQuery);
        builder.append(hqlQuery);
        start = cqi.endPos;
      }
      builder.append(cubeQuery.substring(start));
      String finalQuery = builder.toString();
      hqlCommand.setCommand(finalQuery);
    } catch (SemanticException e) {
      throw new RewriteException("Rewriting failed, cause : " + e.getMessage(), e);
    } catch (ParseException e) {
      throw new RewriteException("Rewriting failed, cause : " + e.getMessage(), e);
    }
    return hqlCommand;
  }

  public static Configuration getDriverQueryConf(LensDriver driver, Configuration queryConf) {
    Configuration conf = new Configuration(driver.getConf());
    for (Map.Entry<String, String> entry : queryConf) {
      if(entry.getKey().equals("cube.query.driver.supported.storages")){
        CubeDriver.LOG.warn("cube.query.driver.supported.storages value : "
            + entry.getValue() + " from query conf ignored/");
        continue;
      }
      conf.set(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  private static CubeQueryRewriter getRewriter(Configuration queryConf) {
    return new CubeQueryRewriter(queryConf);
  }

  public List<CubeQueryInfo> parse() throws SemanticException, ParseException {
    List<CubeQueryInfo> cubeQueries = findCubePositions(getCommand());
    this.cubeQueries = cubeQueries;
    parsed = true;
    return cubeQueries;
  }

  public static List<CubeQueryInfo> findCubePositions(String query) throws SemanticException, ParseException {
    ASTNode ast = HQLParser.parseHQL(query);
    LOG.debug("User query AST:" + ast.dump());
    List<CubeQueryInfo> cubeQueries = new ArrayList<CubeQueryInfo>();
    findCubePositions(ast, cubeQueries, query);
    for (CubeQueryInfo cqi : cubeQueries) {
      cqi.query = query.substring(cqi.startPos, cqi.endPos);
    }
    return cubeQueries;
  }

  private static void findCubePositions(ASTNode ast, List<CubeQueryInfo> cubeQueries,
                                        String originalQuery)
      throws SemanticException {
    int child_count = ast.getChildCount();
    if (ast.getToken() != null) {
      if (ast.getChild(0) != null) {
        LOG.debug("First child:" + ast.getChild(0) + " Type:" + ((ASTNode) ast.getChild(0)).getToken().getType());
      }
      if (ast.getToken().getType() == HiveParser.TOK_QUERY &&
          ((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        LOG.debug("Inside cube clause");
        CubeQueryInfo cqi = new CubeQueryInfo();
        cqi.cubeAST = ast;
        if (ast.getParent() != null) {
          ASTNode parent = (ASTNode) ast.getParent();
          cqi.startPos = ast.getCharPositionInLine();
          int ci = ast.getChildIndex();
          if (parent.getToken() == null ||
              parent.getToken().getType() == HiveParser.TOK_EXPLAIN ||
              parent.getToken().getType() == HiveParser.TOK_CREATETABLE) {
            // Not a sub query
            cqi.endPos = originalQuery.length();
          } else if (parent.getChildCount() > ci + 1) {
            if (parent.getToken().getType() == HiveParser.TOK_SUBQUERY) {
              //less for the next start and for close parenthesis
              cqi.endPos = getEndPos(originalQuery, parent.getChild(ci + 1).getCharPositionInLine(), ")");;
            } else if (parent.getToken().getType() == HiveParser.TOK_UNION) {
              //one less for the next start and less the size of string 'UNION ALL'
              cqi.endPos = getEndPos(originalQuery,
                  parent.getChild(ci + 1).getCharPositionInLine() - 1,
                  "UNION ALL");
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
            cqi.endPos = getEndPos(originalQuery,
                parent.getParent().getChild(1).getCharPositionInLine(), ")", "UNION ALL") ;
          }
        }
        LOG.debug("Adding cqi " + cqi + " query:" + originalQuery.substring(cqi.startPos, cqi.endPos));
        cubeQueries.add(cqi);
      }
      else {
        for (int child_pos = 0; child_pos < child_count; ++child_pos) {
          findCubePositions((ASTNode)ast.getChild(child_pos), cubeQueries, originalQuery);
        }
      }
    } else {
      LOG.warn("Null AST!");
    }
  }

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
}
