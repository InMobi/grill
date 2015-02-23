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
package org.apache.lens.server.api.driver;

<<<<<<< HEAD:lens-server-api/src/test/java/org/apache/lens/server/api/driver/MockQueryContext.java
import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensConf;
import org.apache.lens.server.api.query.AbstractQueryContext;

import java.util.Collection;
=======
import java.util.Collection;

import org.apache.lens.api.LensConf;
import org.apache.lens.server.api.query.AbstractQueryContext;

import org.apache.hadoop.conf.Configuration;
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7:lens-server-api/src/test/java/org/apache/lens/server/api/driver/MockQueryContext.java

public class MockQueryContext extends AbstractQueryContext {

  public MockQueryContext(final String query, final LensConf qconf,
    final Configuration conf, final Collection<LensDriver> drivers) {
    super(query, "testuser", qconf, conf, drivers);
  }
}
