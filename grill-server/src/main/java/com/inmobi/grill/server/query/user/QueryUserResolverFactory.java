package com.inmobi.grill.server.query.user;

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

public class QueryUserResolverFactory {

  public static enum RESOLVER_TYPE {
    FIXED,
    PROPERTYBASED,
    DATABASE,
    CUSTOM
  }
  QueryUserResolver getQueryUserResolver(String resolverType) throws GrillException {
    for(RESOLVER_TYPE type: RESOLVER_TYPE.values()) {
      if(type.name().equals(resolverType)) {
        return getQueryUserResolver(type);
      }
    }
    throw new GrillException("user resolver type not determined. provided value: " + resolverType);
  }
  QueryUserResolver getQueryUserResolver(RESOLVER_TYPE resolverType) {
    switch(resolverType) {
      case PROPERTYBASED:
        return new PropertyBasedQueryUserResolver();
      case DATABASE:
        return new DatabaseQueryUserResolver();
      case CUSTOM:
        return new CustomQueryUserResolver();
      case FIXED :
      default:
        return new FixedQueryUserResolver();
    }
  }
}
