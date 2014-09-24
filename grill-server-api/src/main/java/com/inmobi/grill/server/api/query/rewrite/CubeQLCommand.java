package com.inmobi.grill.server.api.query.rewrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.util.List;

/*
 * #%L
 * Grill API for server and extensions
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

public abstract class CubeQLCommand extends QueryCommand {

  public static class CubeQueryInfo {
    public int startPos;
    public int endPos;
    public String query;
    public ASTNode cubeAST;
  }

  protected CubeQLCommand(String input, String userName, Configuration conf) {
    super(input, userName, conf);
  }

  protected CubeQLCommand() {
  }

  @Override
  public Type getType() {
    return Type.CUBE;
  }

  public static CubeQLCommand get(String input, String userName, Configuration conf) {
    return new CubeQLCommand(input, userName, conf) {
      @Override
      public boolean matches(String line) {
        return true;
      }

      @Override
      public List<CubeQueryInfo> parse() throws ParseException {
        return null;
      }

      @Override
      public HQLCommand rewrite() throws RewriteException {
        return null;
      }
    };
  }

  public abstract List<CubeQueryInfo> parse() throws SemanticException, ParseException;

  public abstract HQLCommand rewrite() throws RewriteException;

}
