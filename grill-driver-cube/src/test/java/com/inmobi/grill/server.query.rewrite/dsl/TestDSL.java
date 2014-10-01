package com.inmobi.grill.server.query.rewrite.dsl;

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
import com.inmobi.grill.server.api.query.rewrite.QueryCommand;
import com.inmobi.grill.server.api.query.rewrite.dsl.DSL;
import com.inmobi.grill.server.api.query.rewrite.dsl.DSLCommand;
import com.inmobi.grill.server.api.query.rewrite.dsl.DSLSemanticException;
import com.inmobi.grill.server.query.rewrite.CubeQLCommandImpl;
import com.inmobi.grill.server.query.rewrite.ParseException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;

/**
 *  Grill server can accept a registered Domain Specific Language.
 *  The Parsing and rewriting of the DSL is the responsibility of the DSL implementation
 *  and can be rewritten to CubeQL/HQL
 */

public class TestDSL implements DSL {

  @Override
  public String getName() {
    return "TestDSL";
  }

  @Override
  public boolean accept(DSLCommand command) throws ParseException, AuthorizationException {
    return command.matches(command.getCommand());
  }

  @Override
  public QueryCommand rewrite(DSLCommand command) throws DSLSemanticException {
    return new CubeQLCommandImpl("cube select name from table", "test", new HiveConf());
  }
}