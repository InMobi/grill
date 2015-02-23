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

<<<<<<< HEAD
import org.apache.hadoop.conf.Configuration;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
=======
import java.util.Collections;
import java.util.Comparator;

import org.apache.lens.api.query.QueryCost;
import org.apache.lens.server.api.query.AbstractQueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

public class MinQueryCostSelector implements DriverSelector {
  public static final Logger LOG = Logger.getLogger(MinQueryCostSelector.class);

  /**
   * Returns the driver that has the minimum query cost.
   *
   * @param ctx  the context
   * @param conf the conf
   * @return the lens driver
   */
  @Override
  public LensDriver select(final AbstractQueryContext ctx,
    final Configuration conf) {
    return Collections.min(ctx.getDriverContext().getDrivers(), new Comparator<LensDriver>() {
      @Override
      public int compare(LensDriver d1, LensDriver d2) {
<<<<<<< HEAD
        return comparePlans(ctx.getDriverContext().getDriverQueryPlan(d1), ctx
          .getDriverContext().getDriverQueryPlan(d2));
=======
        return compareCosts(ctx.getDriverContext().getDriverQueryCost(d1), ctx
          .getDriverContext().getDriverQueryCost(d2));
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
      }
    });
  }

<<<<<<< HEAD
  int comparePlans(DriverQueryPlan c1, DriverQueryPlan c2) {
=======
  int compareCosts(QueryCost c1, QueryCost c2) {
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
    if (c1 == null && c2 == null) {
      return 0;
    } else if (c1 == null && c2 != null) {
      return 1;
    } else if (c1 != null && c2 == null) {
      return -1;
    }
    return c1.compareTo(c2);
  }
}
