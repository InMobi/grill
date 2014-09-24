package com.inmobi.grill.server.user;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.api.GrillConfConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashMap;

/*
 * #%L
 * Grill Server
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
public class UserConfigLoaderTest {
  private HiveConf conf;

  @BeforeTest(alwaysRun = true)
  public void init() {
    conf = new HiveConf();
  }
  @Test
  public void testFixed() throws GrillException {
    conf.addResource(UserConfigLoaderTest.class.getResourceAsStream("/user/fixed.xml"));
    HashMap<String, String> expected = new HashMap<String, String>() {
      {
        put(GrillConfConstants.GRILL_SESSION_CLUSTER_USER, "grilluser");
      }
    };
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user1", conf), expected);
  }

  @Test
  public void testPropertyBased() throws GrillException {
    conf.addResource(UserConfigLoaderTest.class.getResourceAsStream("/user/propertybased.xml"));
    conf.set(GrillConfConstants.GRILL_SESSION_USER_RESOLVER_PROPERTYBASED_FILENAME, UserConfigLoaderTest.class.getResource("/user/propertybased.txt").getPath());
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user1", conf), new HashMap<String, String>() {
      {
        put(GrillConfConstants.GRILL_SESSION_CLUSTER_USER, "clusteruser1");
        put(GrillConfConstants.GRILL_SESSION_QUEUE, "queue1");
      }
    });
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user2", conf), new HashMap<String, String>() {
      {
        put(GrillConfConstants.GRILL_SESSION_CLUSTER_USER, "clusteruser2");
        put(GrillConfConstants.GRILL_SESSION_QUEUE, "queue2");
      }
    });
  }

  @Test
  public void testDatabase() throws GrillException {

  }
  @Test
  public void testCustom() throws GrillException {
    conf.addResource(UserConfigLoaderTest.class.getResourceAsStream("/user/custom.xml"));
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user1", conf), FooBarConfigLoader.CONST_HASH_MAP);
  }
}
