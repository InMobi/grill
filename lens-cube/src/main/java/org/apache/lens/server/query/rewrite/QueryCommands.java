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

import org.apache.lens.server.api.query.rewrite.QueryCommand;
import org.apache.lens.server.query.rewrite.dsl.DSLCommandImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class QueryCommands {

  static final QueryCommand[] commands = new QueryCommand[] {
      new NonSQLCommandImpl(), new DSLCommandImpl(), new CubeQLCommandImpl(), new HQLCommandImpl()
  };

  public static QueryCommand get(String input, String userName, Configuration conf) {
    input = preProcessQuery(input);
    QueryCommand.Type type = getCommandType(input);
    return get(type, input, userName, conf);
  }

  private static QueryCommand get(QueryCommand.Type type, String command, String userName, Configuration conf) {
     switch(type) {
       case CUBE: return new CubeQLCommandImpl(command, userName, conf);
       case NONSQL:return new NonSQLCommandImpl(command, userName, conf);
       case DOMAIN:return new DSLCommandImpl(command, userName, conf);
       case HQL:return new HQLCommandImpl(command, userName, conf);
       default: throw new IllegalStateException("Unknown Query Command : " + command);
     }
  }

  public static String preProcessQuery(final String query) {
    String finalQuery = StringUtils.trim(query).replaceAll("[\\n\\r]", " ")
        .replaceAll("&&", " AND ").replaceAll("\\|\\|", " OR ");
    return finalQuery;
  }

  public static QueryCommand.Type getCommandType(String input) {
    for(QueryCommand cmd : QueryCommands.commands) {
      if (cmd.matches(input)) {
        return cmd.getType();
      }
    }
    throw new IllegalArgumentException("Could not parse query input : " + input);
  }

}
