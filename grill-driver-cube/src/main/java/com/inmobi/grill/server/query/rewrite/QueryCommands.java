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
import com.inmobi.grill.server.api.query.rewrite.QueryCommand;
import com.inmobi.grill.server.query.rewrite.dsl.DSLCommandImpl;
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
    String finalQuery = query.replaceAll("[\\n\\r]", " ")
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
