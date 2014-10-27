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
package org.apache.lens.server.query.rewrite;

import org.apache.lens.api.LensException;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.rewrite.QueryCommand;

import java.util.Collection;
import java.util.Map;

/**
 * Rewrites the given query to Driver Specific HQL
 */
public interface DriverSpecificQueryRewrite {

  /**
   * Initialize the rewriter
   * @param ctx QueryContext
   */
  void init(QueryContext ctx);

  /**
   * Initialize the rewriter
   * @param ctx PreparedQueryContext
   */
  void init(PreparedQueryContext ctx);

  /**
   *
   * @param command query to be rewritten
   * @param drivers Grill drivers for which teh query needs to be rewritten
   * @return
   * @throws LensException
   */
    Map<LensDriver, QueryCommand> rewrite(QueryCommand command, Collection<LensDriver> drivers) throws LensException;
}
