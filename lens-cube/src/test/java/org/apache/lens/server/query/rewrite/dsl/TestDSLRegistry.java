package org.apache.lens.server.query.rewrite.dsl;

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

import org.apache.hadoop.hive.conf.HiveConf;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


@Test(alwaysRun=true, groups="unit-test")
public class TestDSLRegistry {
  HiveConf conf;
  DSLRegistry registry;

  @BeforeTest
  public void beforeTest() throws Exception {
    conf = new HiveConf();
    conf.set("grill.query.dsls", "test");
    conf.set("grill.query.test.dsl.impl", "com.inmobi.grill.server.query.rewrite.dsl.TestDSL");
    registry = DSLRegistry.getInstance();
    registry.init(conf);
  }

  @AfterTest
  public void afterTest() throws Exception {
  }

  @Test
  public void testDSLRegistry() throws Exception {
    Assert.assertNotNull(registry.getDSLs());
    Assert.assertTrue(registry.getDSLs().iterator().next().getClass().equals(TestDSL.class));
  }
}