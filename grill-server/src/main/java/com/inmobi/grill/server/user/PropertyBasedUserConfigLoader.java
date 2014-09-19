package com.inmobi.grill.server.user;
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

import com.inmobi.grill.server.api.GrillConfConstants;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyBasedUserConfigLoader extends UserConfigLoader {

  private HashMap<String, String> userMap;

  public PropertyBasedUserConfigLoader(HiveConf conf) throws UserConfigLoaderException {
    super(conf);
    userMap = new HashMap<String, String>();
    Properties properties = new Properties();
    String filename = hiveConf.get(GrillConfConstants.GRILL_SESSION_USER_RESOLVER_PROPERTYBASED_FILENAME, null);
    if(filename == null) {
      throw new UserConfigLoaderException("property file path not provided for property based resolver." +
        "Please set property " + GrillConfConstants.GRILL_SESSION_USER_RESOLVER_PROPERTYBASED_FILENAME);
    }
    try {
      properties.load(new InputStreamReader(new FileInputStream(new File(
        filename))));
    } catch (IOException e) {
      throw new UserConfigLoaderException("property file not found. Provided path was: " + filename);
    }
    for(Object o: properties.keySet()) {
      String key = (String) o;
      for(String s: key.trim().split("\\s*,\\s*")) {
        userMap.put(s, properties.getProperty(key));
      }
    }
  }
  @Override
  public Map<String, String> getUserConfig(String loggedInUser) {
    HashMap<String, String> userConfig = new HashMap<String, String>();
    userConfig.put(GrillConfConstants.GRILL_SESSION_CLUSTER_USER,
      userMap.get(loggedInUser) == null ?
        hiveConf.get(GrillConfConstants.GRILL_SESSION_USER_RESOLVER_FIXED_VALUE, "grill") :
        userMap.get(loggedInUser));
    return userConfig;
  }
}