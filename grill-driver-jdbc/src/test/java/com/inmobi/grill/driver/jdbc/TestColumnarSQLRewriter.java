package com.inmobi.grill.driver.jdbc;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.testng.Assert;
//import org.junit.AfterClass;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillException;

public class TestColumnarSQLRewriter {

	private void compareQueries(String expected, String actual) {
		if (expected == null && actual == null) {
			return;
		} else if (expected == null) {
			Assert.fail();
		} else if (actual == null) {
			Assert.fail("Rewritten query is null");
		}
		String expectedTrimmed = expected.replaceAll("\\W", "");
		String actualTrimmed = actual.replaceAll("\\W", "");

		if (!expectedTrimmed.equalsIgnoreCase(actualTrimmed)) {
			String method = null;
			for (StackTraceElement trace : Thread.currentThread()
					.getStackTrace()) {
				if (trace.getMethodName().startsWith("test")) {
					method = trace.getMethodName() + ":"
							+ trace.getLineNumber();
				}
			}

			System.err.println("__FAILED__ " + method + "\n\tExpected: "
					+ expected + "\n\t---------\n\tActual: " + actual);
			// System.err.println("\t__AGGR_EXPRS:" +
			// rewrittenQuery.getAggregateExprs());
		}
		Assert.assertTrue(expectedTrimmed.equalsIgnoreCase(actualTrimmed));
	}

	@Test
	public void testJoinCond() throws ParseException, SemanticException,
			GrillException {

		String query =

		"select fact.day_key,account_dim.account_name, "
				+ "case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end "
				+ "from demand_fact fact inner join  account_dim "
				+ "on (fact.advertiser_account_key = account_dim.account_key) "
				+ "inner join os_dim os_dim  on (fact.os_key = os_dim.os_key) and os_dim.os_name = 'ios' "
				+ "where account_dim.account_name = 'supercell' "
				+ "group by fact.day_key,account_dim.account_name "
				+ "order by case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end ";

		SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

		HiveConf conf = new HiveConf();
		ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

		String rwq = qtest.rewrite(conf, query);
		String expected = " inner join os_dim  os_dim  on ((( fact  .  os_key ) = ( os_dim  .  os_key )) "
				+ "and (( os_dim  .  os_name ) =  'ios' )) inner join account_dim  on (( fact  .  advertiser_account_key ) = "
				+ "( account_dim  .  account_key ))";
		String actual = qtest.joinCondition.toString();

		compareQueries(expected, actual);
	}

	@Test
	public void testAllFilterCond() throws ParseException, SemanticException,
			GrillException {

		String query =

		"select fact.day_key,account_dim.account_name, "
				+ "case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end "
				+ "from demand_fact fact inner join  account_dim "
				+ "on (fact.advertiser_account_key = account_dim.account_key) "
				+ "inner join os_dim os_dim  on (fact.os_key = os_dim.os_key) and os_dim.os_name = 'ios' "
				+ "right outer join operator_dim operator_dim on (fact.operator_key = operator_dim.operator_key) and operator_dim.operator_name = '3'"
				+ "full outer join manufacturer_dim manufacturer_dim  on (fact.manufacturer_key = manufacturer_dim.manufacturer_key )  and manufacturer_dim.maufacturer_name = 'apple' "
				+ "where account_dim.account_name = 'supercell' "
				+ "group by fact.day_key,account_dim.account_name "
				+ "order by case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end ";

		SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

		HiveConf conf = new HiveConf();
		ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

		String rwq = qtest.rewrite(conf, query);
		String expected = "[, (( account_dim  .  account_name ) =  'supercell' ), "
				+ "(( manufacturer_dim  .  maufacturer_name ) =  'apple' ), (( os_dim  .  os_name ) =  'ios' ),"
				+ " (( operator_dim  .  operator_name ) =  '3' )]";
		Set<String> setAllFilters = new HashSet<String>(qtest.rightFilter);
		String actual = setAllFilters.toString();
		compareQueries(expected, actual);
	}

	@Test
	public void testAllAggColumn() throws ParseException, SemanticException,
			GrillException {

		String query =

		"select fact.day_key,account_dim.account_name, "
				+ "max(fact.bid_price),avg(fact.cpc),count(fact.fact_count), "
				+ "case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end "
				+ "from demand_fact fact inner join  account_dim "
				+ "on (fact.advertiser_account_key = account_dim.account_key) "
				+ "where account_dim.account_name = 'supercell' "
				+ "group by fact.day_key,account_dim.account_name "
				+ "order by case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end ";

		SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

		HiveConf conf = new HiveConf();
		ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

		String rwq = qtest.rewrite(conf, query);
		String expected = "[count(( fact  .  fact_count )) as count_fact_fact_count, "
				+ "max(( fact  .  bid_price )) as max_fact_bid_price, "
				+ "sum(( fact  .  spend )) as sum_fact_spend, "
				+ "avg(( fact  .  cpc )) as avg_fact_cpc]";
		String actual = qtest.aggColumn.toString();
		compareQueries(expected, actual);
	}

	@Test
	public void testAllFactKeys() throws ParseException, SemanticException,
			GrillException {

		String query =

		"select fact.day_key,account_dim.account_name, "
				+ "max(fact.bid_price),avg(fact.cpc),count(fact.fact_count), "
				+ "case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end "
				+ "from demand_fact fact inner join  account_dim "
				+ "on (fact.advertiser_account_key = account_dim.account_key) and account_dim.primary_email_id is not null "
				+ "inner join os_dim os_dim  on (fact.os_key = os_dim.os_key) and os_dim.os_name = 'ios' "
				+ "right outer join operator_dim operator_dim on (fact.operator_key = operator_dim.operator_key) and operator_dim.operator_name = '3'"
				+ "full outer join manufacturer_dim manufacturer_dim  on (fact.manufacturer_key = manufacturer_dim.manufacturer_key )  and manufacturer_dim.maufacturer_name = 'apple' "
				+ "inner join country_dim country_dim on (fact.served_country_key = country_dim.country_key) "
				+ "inner join date_dim on (fact.day_key = date_dim.day_key) "
				+ "where account_dim.account_name = 'supercell' "
				+ "group by fact.day_key,account_dim.account_name "
				+ "order by case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end ";

		SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

		HiveConf conf = new HiveConf();
		ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

		String rwq = qtest.rewrite(conf, query);
		String expected = "fact.advertiser_account_key,fact.os_key,"
				+ "fact.operator_key,fact.manufacturer_key,"
				+ "fact.served_country_key,fact.day_key,";
		String actual = qtest.factKeys.toString();
		compareQueries(expected, actual);
	}

	@Test
	public void testFactSubQueries() throws ParseException, SemanticException,
			GrillException {

		String query = "select fact.day_key,account_dim.account_name, "
				+ "max(fact.bid_price),avg(fact.cpc),count(fact.fact_count), "
				+ "case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end "
				+ "from demand_fact fact inner join  account_dim "
				+ "on (fact.advertiser_account_key = account_dim.account_key) and account_dim.primary_email_id is not null "
				+ "inner join os_dim os_dim  on (fact.os_key = os_dim.os_key) and os_dim.os_name = 'ios' "
				+ "right outer join operator_dim operator_dim on (fact.operator_key = operator_dim.operator_key) and operator_dim.operator_name = '3'"
				+ "full outer join manufacturer_dim manufacturer_dim  on (fact.manufacturer_key = manufacturer_dim.manufacturer_key )  and manufacturer_dim.maufacturer_name = 'apple' "
				+ "inner join country_dim country_dim on (fact.served_country_key = country_dim.country_key) "
				+ "inner join date_dim on (fact.day_key = date_dim.day_key) "
				+ "where account_dim.account_name = 'supercell' "
				+ "group by fact.day_key,account_dim.account_name "
				+ "order by case when sum(fact.spend) = 0 then 0.0 else sum(fact.spend) end ";

		SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

		HiveConf conf = new HiveConf();
		ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

		String rwq = qtest.rewrite(conf, query);
		String expected = "fact.advertiser_account_key in  (  select account_dim.account_key from "
				+ "account_dim where (( account_dim  .  account_name ) =  'supercell' ) "
				+ "and ( account_dim  .  primary_email_id ) is not null  ) "
				+ "and fact.os_key in  (  select os_dim.os_key from os_dim where (( os_dim  .  os_name ) =  'ios' ) ) "
				+ "and fact.operator_key in  (  select operator_dim.operator_key from operator_dim "
				+ "where (( operator_dim  .  operator_name ) =  '3' ) ) "
				+ "and fact.manufacturer_key in  (  select manufacturer_dim.manufacturer_key "
				+ "from manufacturer_dim where (( manufacturer_dim  .  maufacturer_name ) =  'apple' ) ) and ";
		String actual = qtest.allSubQueries.toString();
		compareQueries(expected, actual);
	}

	@Test
	public void testRewrittenQuery() throws ParseException, SemanticException,
			GrillException {

		String query = "select fact.day_key,account_dim.account_name, sum(fact.spend), "
				+ "max(fact.bid_price),avg(fact.cpc),count(fact.fact_count) "
				+ "from demand_fact fact inner join  account_dim "
				+ "on (fact.advertiser_account_key = account_dim.account_key) and account_dim.primary_email_id is not null "
				+ "inner join os_dim os_dim  on (fact.os_key = os_dim.os_key) and os_dim.os_name = 'ios' "
				+ "where account_dim.account_name = 'supercell' "
				+ "group by fact.day_key,account_dim.account_name "
				+ "order by sum(fact.spend)";

		SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

		HiveConf conf = new HiveConf();
		ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

		String rwq = qtest.rewrite(conf, query);
		String expected = "select ( fact  .  day_key ), ( account_dim  .  account_name ), "
				+ "sum(sum_fact_spend), max(max_fact_bid_price), avg(avg_fact_cpc), "
				+ "count(count_fact_fact_count) from  (select fact.advertiser_account_key,fact.os_key,"
				+ "count(( fact  .  fact_count )) as count_fact_fact_count, max(( fact  .  bid_price )) "
				+ "as max_fact_bid_price, sum(( fact  .  spend )) as sum_fact_spend, avg(( fact  .  cpc )) as "
				+ "avg_fact_cpc from demand_fact fact where fact.advertiser_account_key in "
				+ " (  select account_dim.account_key from account_dim where (( account_dim  .  account_name ) =  'supercell' ) "
				+ "and ( account_dim  .  primary_email_id ) is not null  ) and fact.os_key in  "
				+ "(  select os_dim.os_key from os_dim where (( os_dim  .  os_name ) =  'ios' ) )  "
				+ "group by fact.advertiser_account_key,fact.os_key) fact inner join os_dim  os_dim  "
				+ "on ((( fact  .  os_key ) = ( os_dim  .  os_key )) and (( os_dim  .  os_name ) =  'ios' )) "
				+ "inner join account_dim  on ((( fact  .  advertiser_account_key ) = ( account_dim  .  account_key )) "
				+ "and ( account_dim  .  primary_email_id ) is not null ) where (( account_dim  .  account_name ) =  'supercell' ) "
				+ "group by ( fact  .  day_key ), ( account_dim  .  account_name ) "
				+ "order by sum(sum_fact_spend)";
		String actual = qtest.finalRewrittenQuery;
		compareQueries(expected, actual);
	}

	@Test
	public void testUnionQuery() throws ParseException, SemanticException,
			GrillException {

		String query = "select fact.day_key,account_dim.account_name, sum(fact.spend), "
				+ "max(fact.bid_price),avg(fact.cpc),count(fact.fact_count) "
				+ "from demand_fact fact inner join  account_dim "
				+ "on (fact.advertiser_account_key = account_dim.account_key) and account_dim.primary_email_id is not null "
				+ "inner join os_dim os_dim  on (fact.os_key = os_dim.os_key) and os_dim.os_name = 'ios' "
				+ "inner join date_dim date_dim on (fact.day_key = date_dim.day_key) "
				+ "where account_dim.account_name = 'supercell' "
				+ "and date_dim.full_date between '2014-01-01' and '2014-01-04'"
				+ "group by fact.day_key,account_dim.account_name "
				+ "union all "
				+ "select fact.day_key,account_dim.account_name, sum(fact.spend), "
				+ "max(fact.bid_price),avg(fact.cpc),count(fact.fact_count) "
				+ "from demand_fact fact inner join  account_dim "
				+ "on (fact.advertiser_account_key = account_dim.account_key) and account_dim.primary_email_id is not null "
				+ "inner join os_dim os_dim  on (fact.os_key = os_dim.os_key) and os_dim.os_name = 'ios' "
				+ "inner join date_dim date_dim on (fact.day_key = date_dim.day_key) "
				+ "where account_dim.account_name = 'supercell' "
				+ "and date_dim.full_date between '2014-02-01' and '2014-02-04' "
				+ "group by fact.day_key,account_dim.account_name "
				+ "union all "
				+ "select fact.day_key,account_dim.account_name, sum(fact.spend), "
				+ "max(fact.bid_price),avg(fact.cpc),count(fact.fact_count) "
				+ "from demand_fact fact inner join  account_dim "
				+ "on (fact.advertiser_account_key = account_dim.account_key) and account_dim.primary_email_id is not null "
				+ "inner join os_dim os_dim  on (fact.os_key = os_dim.os_key) and os_dim.os_name = 'ios' "
				+ "inner join date_dim date_dim on (fact.day_key = date_dim.day_key) "
				+ "where account_dim.account_name = 'supercell' "
				+ "and date_dim.full_date between '2014-03-01' and '2014-03-04' "
				+ "group by fact.day_key,account_dim.account_name ";

		SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

		HiveConf conf = new HiveConf();
		ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

		String rwq = qtest.rewrite(conf, query);
		String expected = "select ( fact  .  day_key ), ( account_dim  .  account_name ), sum(sum_fact_spend), max(max_fact_bid_price), avg(avg_fact_cpc), "
				+ "count(count_fact_fact_count) from  (select fact.advertiser_account_key,fact.os_key,fact.day_key,count(( fact  .  fact_count )) as "
				+ "count_fact_fact_count, max(( fact  .  bid_price )) as max_fact_bid_price, sum(( fact  .  spend )) as sum_fact_spend, avg(( fact  .  cpc )) "
				+ "as avg_fact_cpc from demand_fact fact where fact.advertiser_account_key in  (  select account_dim.account_key from account_dim where"
				+ " (( account_dim  .  account_name ) =  'supercell' ) and ( account_dim  .  primary_email_id ) is not null  ) and fact.os_key in  "
				+ "(  select os_dim.os_key from os_dim where (( os_dim  .  os_name ) =  'ios' ) ) and fact.day_key in "
				+ " (  select date_dim.day_key from date_dim where ( date_dim  .  full_date ) between  '2014-01-01'  and  '2014-01-04'  )  "
				+ "group by fact.advertiser_account_key,fact.os_key,fact.day_key) fact inner join date_dim  date_dim  on (( fact  .  day_key ) = "
				+ "( date_dim  .  day_key )) inner join os_dim  os_dim  on ((( fact  .  os_key ) = ( os_dim  .  os_key )) and (( os_dim  .  os_name ) =  'ios' ))"
				+ " inner join account_dim  on ((( fact  .  advertiser_account_key ) = ( account_dim  .  account_key )) and "
				+ "( account_dim  .  primary_email_id ) is not null ) where ((( account_dim  .  account_name ) =  'supercell' ) "
				+ "and ( date_dim  .  full_date ) between  '2014-01-01'  and  '2014-01-04' ) group by ( fact  .  day_key ), "
				+ "( account_dim  .  account_name ) union all select ( fact  .  day_key ), ( account_dim  .  account_name ), "
				+ "sum(sum_fact_spend), max(max_fact_bid_price), avg(avg_fact_cpc), count(count_fact_fact_count) from  "
				+ "(select fact.advertiser_account_key,fact.os_key,fact.day_key,count(( fact  .  fact_count )) as "
				+ "count_fact_fact_count, max(( fact  .  bid_price )) as max_fact_bid_price, sum(( fact  .  spend )) as sum_fact_spend, "
				+ "avg(( fact  .  cpc )) as avg_fact_cpc from demand_fact fact where fact.advertiser_account_key in  "
				+ "(  select account_dim.account_key from account_dim where (( account_dim  .  account_name ) =  'supercell' ) "
				+ "and ( account_dim  .  primary_email_id ) is not null  ) and fact.os_key in  (  select os_dim.os_key from os_dim where "
				+ "(( os_dim  .  os_name ) =  'ios' ) ) and fact.day_key in  (  select date_dim.day_key from date_dim where ( date_dim  .  full_date ) "
				+ "between  '2014-02-01'  and  '2014-02-04'  )  group by fact.advertiser_account_key,fact.os_key,fact.day_key) "
				+ "fact inner join date_dim  date_dim  on (( fact  .  day_key ) = ( date_dim  .  day_key )) inner join os_dim  os_dim  "
				+ "on ((( fact  .  os_key ) = ( os_dim  .  os_key )) and (( os_dim  .  os_name ) =  'ios' )) inner join account_dim  "
				+ "on ((( fact  .  advertiser_account_key ) = ( account_dim  .  account_key )) and ( account_dim  .  primary_email_id ) is not null ) "
				+ "where ((( account_dim  .  account_name ) =  'supercell' ) and ( date_dim  .  full_date ) between  '2014-02-01'  and  '2014-02-04' ) "
				+ "group by ( fact  .  day_key ), ( account_dim  .  account_name ) union all "
				+ "select ( fact  .  day_key ), ( account_dim  .  account_name ), sum(sum_fact_spend), "
				+ "max(max_fact_bid_price), avg(avg_fact_cpc), count(count_fact_fact_count) from  "
				+ "(select fact.advertiser_account_key,fact.os_key,fact.day_key,count(( fact  .  fact_count )) as "
				+ "count_fact_fact_count, max(( fact  .  bid_price )) as max_fact_bid_price, sum(( fact  .  spend )) as sum_fact_spend, "
				+ "avg(( fact  .  cpc )) as avg_fact_cpc from demand_fact fact where fact.advertiser_account_key in  "
				+ "(  select account_dim.account_key from account_dim where (( account_dim  .  account_name ) =  'supercell' ) "
				+ "and ( account_dim  .  primary_email_id ) is not null  ) and fact.os_key in  (  select os_dim.os_key from os_dim where "
				+ "(( os_dim  .  os_name ) =  'ios' ) ) and fact.day_key in  (  select date_dim.day_key from date_dim "
				+ "where ( date_dim  .  full_date ) between  '2014-03-01'  and  '2014-03-04'  )  group by fact.advertiser_account_key,fact.os_key,fact.day_key) "
				+ "fact inner join date_dim  date_dim  on (( fact  .  day_key ) = ( date_dim  .  day_key )) inner join os_dim  "
				+ "os_dim  on ((( fact  .  os_key ) = ( os_dim  .  os_key )) and (( os_dim  .  os_name ) =  'ios' )) inner join account_dim  "
				+ "on ((( fact  .  advertiser_account_key ) = ( account_dim  .  account_key )) and ( account_dim  .  primary_email_id ) is not null ) "
				+ "where ((( account_dim  .  account_name ) =  'supercell' ) and ( date_dim  .  full_date ) between  '2014-03-01'  and  '2014-03-04' ) "
				+ "group by ( fact  .  day_key ), ( account_dim  .  account_name ) ";
		String actual = qtest.finalRewrittenQuery.toString();
		compareQueries(expected, actual);
	}
}
