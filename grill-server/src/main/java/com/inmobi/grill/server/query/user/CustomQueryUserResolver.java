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

import com.inmobi.grill.server.api.GrillConfConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class CustomQueryUserResolver extends QueryUserResolver {

  Class<? extends QueryUserResolver> customHandlerClass;
  QueryUserResolver customProvider;

  public CustomQueryUserResolver() {
    this.customHandlerClass = (Class<? extends QueryUserResolver>) hiveConf.getClass(
      GrillConfConstants.GRILL_QUERY_USER_RESOLVER_CUSTOM_CLASS,
      QueryUserResolver.class
    );
    this.customProvider =
      ReflectionUtils.newInstance(this.customHandlerClass, new Configuration());
  }

  @Override
  public String resolve(String loggedInUser) {
    return customProvider.resolve(loggedInUser);
  }
}
