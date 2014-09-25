package com.inmobi.grill.server.api.driver;

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

import java.util.Collection;
import java.util.Map;

import com.inmobi.grill.server.api.query.rewrite.HQLCommand;
import com.inmobi.grill.server.api.query.rewrite.QueryCommand;
import org.apache.hadoop.conf.Configuration;

public interface DriverSelector {
  public GrillDriver select(Collection<GrillDriver> drivers,
      Map<GrillDriver, QueryCommand> queries, Configuration conf);
}
