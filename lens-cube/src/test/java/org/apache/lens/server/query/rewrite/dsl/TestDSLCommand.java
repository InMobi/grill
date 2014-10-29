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
package org.apache.lens.server.query.rewrite.dsl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.query.rewrite.QueryCommand;
import org.apache.lens.server.api.query.rewrite.dsl.DSLCommand;
import org.apache.lens.server.api.query.rewrite.dsl.DSLSemanticException;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestDSLCommand {


  HiveConf conf = new HiveConf();
  DSLRegistry registry;

  @BeforeTest
  public void beforeTest() throws Exception {
    conf.set("grill.server.query.rewriter", "com.inmobi.grill.server.query.rewrite.DriverSpecificQueryRewriterImpl");
    registry = DSLRegistry.getInstance();
    registry.init(conf);
  }

  @AfterTest
  public void afterTest() throws Exception {
  }

  @Test
  public void testDSLValidCommand() throws Exception {
    final String TEST_COMMAND="domain select * from domain_entity";
    final String REWRITTEN_COMMAND="cube select name from table";
    DSLCommand command = new DSLCommandImpl(TEST_COMMAND, null, conf);

    Assert.assertTrue(command.matches(TEST_COMMAND));

    final QueryCommand rewrittenQuery = command.rewrite();
    Assert.assertEquals(rewrittenQuery.getCommand(), REWRITTEN_COMMAND);
  }

  @Test
  public void testDSLInValidCommand() throws Exception {

    final String TEST_COMMAND="select * from domain_entity";
    DSLCommand command = new DSLCommandImpl(TEST_COMMAND, null, conf);
    Assert.assertFalse(command.matches("check invalid"));
    try {
      command.rewrite();
      Assert.fail("Expected DSLSemanticException during query rewrite!");
    }catch(DSLSemanticException dse) {
    }
  }
}
