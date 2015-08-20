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

package org.apache.lens.cube.parse;

import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.FACT_NOT_AVAILABLE_IN_RANGE;
import static org.apache.lens.cube.parse.CubeTestSetup.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestTimeRangeResolver extends TestQueryRewrite {

  private final String cubeName = CubeTestSetup.TEST_CUBE_NAME;

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Override
  public Configuration getConf() {
    return new Configuration(conf);
  }

  @Test
  public void testFactValidity() throws ParseException, LensException, HiveException, ClassNotFoundException {
    LensException e =
      getLensExceptionInRewrite("cube select msr2 from " + cubeName + " where " + LAST_YEAR_RANGE,
        getConf());
    PruneCauses.BriefAndDetailedError causes = extractPruneCause(e);
    assertTrue(causes.getBrief().contains("Columns [msr2] are not present in any table"));
    assertEquals(causes.getDetails().size(), 2);
    assertEquals(causes.getDetails().values().iterator().next().size(), 1);
    assertEquals(causes.getDetails().values().iterator().next().iterator().next().getCause(),
      FACT_NOT_AVAILABLE_IN_RANGE);
  }

  @Test
  public void testAbsoluteValidity() throws ParseException, HiveException, LensException {
    CubeQueryContext ctx =
      rewriteCtx("cube select msr12 from basecube where " + TWO_DAYS_RANGE + " or " + TWO_DAYS_RANGE_BEFORE_4_DAYS,
        getConf());
    assertEquals(ctx.getFactPruningMsgs().get(ctx.getMetastoreClient().getCubeFact("testfact_deprecated")).size(), 1);
    CandidateTablePruneCause pruningMsg =
      ctx.getFactPruningMsgs().get(ctx.getMetastoreClient().getCubeFact("testfact_deprecated")).get(0);
    // testfact_deprecated's validity should be in between of both ranges. So both ranges should be in the invalid list
    // That would prove that parsing of properties has gone through successfully
    assertEquals(pruningMsg.getCause(), FACT_NOT_AVAILABLE_IN_RANGE);
    assertTrue(pruningMsg.getInvalidRanges().containsAll(ctx.getTimeRanges()));
  }
}
