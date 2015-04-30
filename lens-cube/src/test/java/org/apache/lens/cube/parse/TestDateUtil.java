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

import static org.apache.lens.cube.metadata.UpdatePeriod.*;
import static org.apache.lens.cube.parse.DateUtil.*;

import static org.apache.commons.lang.time.DateUtils.addMilliseconds;

import static org.testng.Assert.assertEquals;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.lens.cube.parse.DateUtil.*;

import org.apache.commons.lang.time.DateUtils;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Unit tests for cube DateUtil class TestDateUtil.
 */
public class TestDateUtil {
  public static final String[] TEST_PAIRS = {
    "2013-Jan-01", "2013-Jan-31", "2013-Jan-01", "2013-May-31",
    "2013-Jan-01", "2013-Dec-31", "2013-Feb-01", "2013-Apr-25",
    "2012-Feb-01", "2013-Feb-01", "2011-Feb-01", "2013-Feb-01",
    "2013-Jan-02", "2013-Feb-02", "2013-Jan-02", "2013-Mar-02",
  };

  public static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MMM-dd");
  public static final String ABS_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss:SSS";

  public static final ThreadLocal<DateFormat> ABS_DATE_PARSER =
    new ThreadLocal<DateFormat>() {
      @Override
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(ABS_DATE_FORMAT);
      }
    };

  private Date[] pairs;

  @BeforeTest
  public void setUp() {
    pairs = new Date[TEST_PAIRS.length];
    for (int i = 0; i < TEST_PAIRS.length; i++) {
      try {
        pairs[i] = DATE_FMT.parse(TEST_PAIRS[i]);
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testMonthsBetween() throws Exception {
    int i = 0;
    assertEquals(getMonthlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], Calendar.MONTH)),
      new CoveringInfo(1, true),
      "2013-Jan-01 to 2013-Jan-31");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], Calendar.MONTH)),
      new CoveringInfo(5, true),
      "2013-Jan-01 to 2013-May-31");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], Calendar.MONTH)),
      new CoveringInfo(12, true),
      "2013-Jan-01 to 2013-Dec-31");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(2, false),
      "2013-Feb-01 to 2013-Apr-25");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(12, true),
      "2012-Feb-01 to 2013-Feb-01");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(24, true),
      "2011-Feb-01 to 2013-Feb-01");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "2013-Jan-02 to 2013-Feb-02");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(1, false),
      "2013-Jan-02 to 2013-Mar-02");
  }

  @Test
  public void testQuartersBetween() throws Exception {
    int i = 0;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "2013-Jan-01 to 2013-Jan-31");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(1, false),
      "2013-Jan-01 to 2013-May-31");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], Calendar.MONTH)),
      new CoveringInfo(4, true),
      "2013-Jan-01 to 2013-Dec-31");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "2013-Feb-01 to 2013-Apr-25");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(3, false),
      "2012-Feb-01 to 2013-Feb-01");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(7, false),
      "2011-Feb-01 to 2013-Feb-01");
  }

  @Test
  public void testYearsBetween() throws Exception {
    int i = 0;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "" + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "" + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], Calendar.MONTH)),
      new CoveringInfo(1, true), ""
        + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "" + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "" + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(1, false),
      "" + pairs[i] + "->" + pairs[i + 1]);
  }

  @Test
  public void testWeeksBetween() throws Exception {
    CoveringInfo weeks;

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-26"), DATE_FMT.parse("2013-Jun-2"));
    assertEquals(weeks, new CoveringInfo(1, true), "2013-May-26 to 2013-Jun-2");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-27"), DATE_FMT.parse("2013-Jun-3"));
    assertEquals(weeks, new CoveringInfo(0, false), "2013-May-26 to 2013-Jun-2");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-27"), DATE_FMT.parse("2013-Jun-9"));
    assertEquals(weeks, new CoveringInfo(1, false), "2013-May-26 to 2013-Jun-2");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-27"), DATE_FMT.parse("2013-Jun-1"));
    assertEquals(weeks, new CoveringInfo(0, false), "2013-May-27 to 2013-Jun-1");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-25"), DATE_FMT.parse("2013-Jun-2"));
    assertEquals(weeks, new CoveringInfo(1, false), "2013-May-25 to 2013-Jun-1");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-26"), DATE_FMT.parse("2013-Jun-9"));
    assertEquals(weeks, new CoveringInfo(2, true), "2013-May-26 to 2013-Jun-8");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-26"), DATE_FMT.parse("2013-Jun-10"));
    assertEquals(weeks, new CoveringInfo(2, false), "2013-May-26 to 2013-Jun-10");
  }

  @Test
  public void testNowWithGranularity() throws Exception {
    String dateFmt = "yyyy/MM/dd-HH.mm.ss.SSS";
    // Tuesday Sept 23, 2014, 12.02.05.500 pm
    String testDateStr = "2014/09/23-12.02.05.500";
    final SimpleDateFormat sdf = new SimpleDateFormat(dateFmt);
    final Date testDate = sdf.parse(testDateStr);

    System.out.print("@@ testDateStr=" + testDateStr + " parsed date=" + testDate);

    // Tests without a diff, just resolve now with different granularity
    assertEquals(testDateStr, sdf.format(resolveDate("now", testDate)));
    assertEquals("2014/01/01-00.00.00.000", sdf.format(resolveDate("now.year", testDate)));
    assertEquals("2014/09/01-00.00.00.000", sdf.format(resolveDate("now.month", testDate)));
    // Start of week resolves to Sunday
    assertEquals("2014/09/21-00.00.00.000", sdf.format(resolveDate("now.week", testDate)));
    assertEquals("2014/09/23-00.00.00.000", sdf.format(resolveDate("now.day", testDate)));
    assertEquals("2014/09/23-12.00.00.000", sdf.format(resolveDate("now.hour", testDate)));
    assertEquals("2014/09/23-12.02.00.000", sdf.format(resolveDate("now.minute", testDate)));
    assertEquals("2014/09/23-12.02.05.000", sdf.format(resolveDate("now.second", testDate)));

    // Tests with a diff
    assertEquals("2014/09/22-00.00.00.000", sdf.format(resolveDate("now.day -1day", testDate)));
    assertEquals("2014/09/23-10.00.00.000", sdf.format(resolveDate("now.hour -2hour", testDate)));
    assertEquals("2014/09/24-12.00.00.000", sdf.format(resolveDate("now.hour +24hour", testDate)));
    assertEquals("2015/01/01-00.00.00.000", sdf.format(resolveDate("now.year +1year", testDate)));
    assertEquals("2014/02/01-00.00.00.000", sdf.format(resolveDate("now.year +1month", testDate)));
  }

  @Test
  public void testFloorDate() throws ParseException {
    Date date = ABS_DATE_PARSER.get().parse("2015-01-01T00:00:00:000Z");
    Date curDate = date;
    for (int i = 0; i < 284; i++) {
      assertEquals(getFloorDate(curDate, YEARLY), date);
      curDate = addMilliseconds(curDate, 111111111);
    }
    assertEquals(getFloorDate(curDate, YEARLY), DateUtils.addYears(date, 1));
    assertEquals(getFloorDate(date, WEEKLY), ABS_DATE_PARSER.get().parse("2014-12-28T00:00:00:000Z"));
  }

  @Test
  public void testCeilDate() throws ParseException {
    Date date = ABS_DATE_PARSER.get().parse("2015-12-26T06:30:15:040Z");
    assertEquals(getCeilDate(date, YEARLY), ABS_DATE_PARSER.get().parse("2016-01-01T00:00:00:000Z"));
    assertEquals(getCeilDate(date, MONTHLY), ABS_DATE_PARSER.get().parse("2016-01-01T00:00:00:000Z"));
    assertEquals(getCeilDate(date, DAILY), ABS_DATE_PARSER.get().parse("2015-12-27T00:00:00:000Z"));
    assertEquals(getCeilDate(date, HOURLY), ABS_DATE_PARSER.get().parse("2015-12-26T07:00:00:000Z"));
    assertEquals(getCeilDate(date, MINUTELY), ABS_DATE_PARSER.get().parse("2015-12-26T06:31:00:000Z"));
    assertEquals(getCeilDate(date, SECONDLY), ABS_DATE_PARSER.get().parse("2015-12-26T06:30:16:000Z"));
    assertEquals(getCeilDate(date, WEEKLY), ABS_DATE_PARSER.get().parse("2015-12-27T00:00:00:000Z"));
  }
}
