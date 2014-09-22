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

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.api.GrillConfConstants;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

public class UserConfigLoaderFactory {


  public static enum RESOLVER_TYPE {
    FIXED,
    PROPERTYBASED,
    DATABASE,
    CUSTOM
  }
  public static UserConfigLoader getQueryUserResolver(HiveConf conf) throws GrillException {
    String resolverType = conf.get(GrillConfConstants.GRILL_SESSION_USER_RESOLVER_TYPE);
    if(resolverType == null || resolverType.length() == 0) {
      throw new GrillException("user resolver type not determined. value was not provided in conf");
    }
    for(RESOLVER_TYPE type: RESOLVER_TYPE.values()) {
      if(type.name().equals(resolverType)) {
        return getQueryUserResolver(type, conf);
      }
    }
    throw new GrillException("user resolver type not determined. provided value: " + resolverType);
  }
  public static UserConfigLoader getQueryUserResolver(RESOLVER_TYPE resolverType, HiveConf conf) throws GrillException {
    switch(resolverType) {
      case PROPERTYBASED:
        return new PropertyBasedUserConfigLoader(conf);
      case DATABASE:
        return new DatabaseUserConfigLoader(conf);
      case CUSTOM:
        return new CustomUserConfigLoader(conf);
      case FIXED :
      default:
        return new FixedUserConfigLoader(conf);
    }
  }
  public static Map<String, String> getUserConfig(String loggedInUser, HiveConf conf) throws GrillException {
    return getQueryUserResolver(conf)
      .getUserConfig(loggedInUser);
  }
}
