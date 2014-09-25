package com.inmobi.grill.server.api.query.rewrite;

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

import org.apache.hadoop.conf.Configuration;

public abstract class HQLCommand extends QueryCommand {

  protected HQLCommand(String input, String userName, Configuration conf) {
    super(input, userName, conf);
  }

  protected HQLCommand() {
  }

  @Override
  public Type getType() {
    return Type.HQL;
  }

  public static HQLCommand get(String input, String userName, Configuration conf) {
    return new HQLCommand(input, userName, conf) {
      @Override
      public boolean matches(String line) {
        return true;
      }

      @Override
      public QueryCommand rewrite() throws RewriteException {
        return null;
      }
    };
  }

  /**
   * Any command that is not matched by CubeQL, DSL, NonSQL is assumed to be HQL by default
   * @param line
   * @return
   */
  @Override
  public boolean matches(String line) {
    return true;
  }
}
