package org.apache.lens.jdbc;

/*
 * #%L
 * Grill client
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

import org.apache.lens.client.GrillClientConfig;
import org.apache.lens.client.GrillConnectionParams;
import org.apache.lens.client.jdbc.JDBCUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


import java.util.Map;

public class JDBCUrlParserTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIllegalJDBCUri() {
    String uri = "jdbc:gril://localhost:1000";
    JDBCUtils.parseUrl(uri);
    Assert.fail("Illegal argument exception should have been thrown.");
  }


  @Test
  public void testDefaultsWithConfigurationVariables() {
    String uri = "jdbc:lens:///;username=johndoe;password=blah?conf1=blah1;conf2=blah2#var1=123;var2=456";
    GrillConnectionParams params = JDBCUtils.parseUrl(uri);
    Assert.assertEquals(
        GrillClientConfig.DEFAULT_DBNAME_VALUE, params.getDbName(),"The database should be default database");
    Assert.assertEquals(
        GrillClientConfig.DEFAULT_SERVER_BASE_URL, params.getBaseConnectionUrl(),"The base url should be default");

    Map<String, String> sessionVars = params.getSessionVars();
    Assert.assertEquals( 2,
        sessionVars.size(),"You should have two session variable");
    Assert.assertEquals("johndoe",
        sessionVars.get("username"),"The username should be johndoe");
    Assert.assertEquals("blah",
        sessionVars.get("password"),"The password should be blah");

    Map<String, String> grillConf = params.getGrillConfs();
    Assert.assertEquals(2,
        grillConf.size(),"You should have two configuration variables");
    Assert.assertEquals( "blah1",
        grillConf.get("conf1"),"The value for conf1 should be blah1");
    Assert.assertEquals( "blah2",
        grillConf.get("conf2"),"The value for conf2 should be blah2");

    Map<String, String> grillVars = params.getGrillVars();

    Assert.assertEquals( 2,
        grillVars.size(),"You should have two grill variables");
    Assert.assertEquals( "123",
        grillVars.get("var1"),"The value for var1 should be 123");
    Assert.assertEquals("456",
        grillVars.get("var2"),"The value for var2 should be 456");
  }

  @Test
  public void testJDBCWithCustomHostAndPortAndDB() {
    String uri = "jdbc:lens://myhost:9000/mydb";
    GrillConnectionParams params = JDBCUtils.parseUrl(uri);
    //Assert.assertEquals( "myhost",
    //    params.getHost(),"The host name should be myhost");
    //Assert.assertEquals( 9000, params.getPort(),"The port should be 9000");
    Assert.assertEquals("mydb",
        params.getDbName(),"The database should be mydb");
    Assert.assertTrue(
        params.getSessionVars().isEmpty(),"Session Variable list should be empty");
    Assert.assertTrue(
        params.getGrillConfs().isEmpty(),"The conf list should be empty");
    Assert.assertTrue(
        params.getGrillVars().isEmpty(),"The grill var list should be empty");
  }
}
