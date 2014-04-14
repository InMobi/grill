package com.inmobi.grill.driver.jdbc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hsqldb.persist.ScriptRunner;
import org.hsqldb.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.api.query.ResultColumnType;
import com.inmobi.grill.api.query.ResultRow;
import com.inmobi.grill.server.api.driver.GrillResultSet;
import com.inmobi.grill.server.api.driver.GrillResultSetMetadata;
import com.inmobi.grill.server.api.driver.InMemoryResultSet;
import com.inmobi.grill.server.api.driver.QueryCompletionListener;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryContext;

public class TestJDBCFinal {
	Configuration baseConf;
	JDBCDriver driver;

	@BeforeTest
	//Create database - grill_jdbc, user - jdbc_test with password - jdbc_test
	//Grant r/w permission on grill_jdbc database to user jdbc_test
	public void testCreateJdbcDriver() throws Exception {
		baseConf = new Configuration();
		baseConf.set(JDBCDriverConfConstants.JDBC_DRIVER_CLASS,
				"com.mysql.jdbc.Driver");
		baseConf.set(JDBCDriverConfConstants.JDBC_DB_URI,
				"jdbc:mysql://localhost:3306/grill_jdbc");
		baseConf.set(JDBCDriverConfConstants.JDBC_USER, "jdbc_test");
		baseConf.set(JDBCDriverConfConstants.JDBC_PASSWORD, "jdbc_test");
		baseConf.set(JDBCDriverConfConstants.JDBC_QUERY_REWRITER_CLASS,
				ColumnarSQLRewriter.class.getName());

		driver = new JDBCDriver();
		driver.configure(baseConf);
		assertNotNull(driver);
		assertTrue(driver.configured);
		System.out.println("Driver configured!");
		SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

		// Configuration conf = new Configuration();
		// ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

	}

	@AfterTest
	public void close() throws Exception {
		driver.close();
		System.out.println("Driver closed!");
	}

	// create table and insert data
	synchronized void createTables() throws Exception {
		Connection conn = null;
		Statement stmt = null;

		String dropTables = "drop table if exists test_fact_jdbc,account_dim_jdbc,country_dim_jdbc,"
				+ "operator_dim_jdbc,date_dim_jdbc";
		String createFact = "create table if not exists test_fact_jdbc  (day_key int, advertiser_account_key bigint,"
				+ "served_country_key bigint,operator_key bigint,"
				+ "spend double, billed_clicks bigint)";
		String createDim1 = "create table if not exists account_dim_jdbc  ( account_key bigint,"
				+ "account_id varchar(200),account_name varchar(1000),account_email_id varchar(850), "
				+ "account_type varchar(200), managed_type varchar(200))";
		String createDim2 = "create table if not exists country_dim_jdbc  ( country_key bigint, "
				+ "country_name varchar(500),geographic_region_name varchar(500))";

		String createDim3 = "create table if not exists operator_dim_jdbc (operator_key bigint, operator_name varchar(500))";

		String createDim4 = "create table if not exists date_dim_jdbc (day_key bigint, full_date datetime)";

		String insertFact = "insert into test_fact_jdbc values "
				+ " (1000,345,110,234,3456.67,67890),"
				+ " (1001,346,111,235,3457.67,67891),"
				+ " (1002,347,112,236,3457.67,67891),"
				+ " (1003,348,113,237,3457.67,67891),"
				+ " (1004,349,114,238,3457.67,67891),"
				+ " (1005,350,115,239,3457.67,67891)";
		String insertDim1 = "insert into account_dim_jdbc values "
				+ "(345,'4028cb90254e69cd01257fb34d1e05e7','Loadmob','admin@loadmob.com','Brand','DSO'),"
				+ "(346,'4028cbe0373b25ce013755f1e3840369','Musicplode Ltd','paul@gameboxco.com','Performance','GOIB'),"
				+ "(347,'4028cb9732fbbbe90132feccfb05014c','morrison software','kimhwan97@gmail.com','Performance','DSO'),"
				+ "(348,'4028cb8b2bedad5c012bf96be22e00fe','Pixel Magic Co., Ltd.','ethan@i-walkernet.com','Performance','DSO'),"
				+ "(349,'4028cb9732fbbbe90132feccfb05014c','morrison software','kimhwan97@gmail.com','Brand','DSO')";

		String insertDim2 = "insert into country_dim_jdbc values "
				+ "(110,'India','JAPAC')," + "(111,'UK','EMEA'),"
				+ "(112,'USA','Nortth America')," + "(113,'Japan','JAPAC'),"
				+ "(114,'Sri Lanka','JAPAC')," + "(115,'Spain','EMEA')";

		String insertDim3 = "insert into operator_dim_jdbc values "
				+ "(234,'AT&T')," + "(235,'Airtel')," + "(236,'Idea'),"
				+ "(237,'3')," + "(238,'Vodafone')," + "(239,'MTS')";

		String insertDim4 = "insert into date_dim_jdbc values "
				+ "(1000,'2014-01-01')," + "(1001,'2014-01-02'),"
				+ "(1002,'2014-01-03')," + "(1003,'2014-01-04'),"
				+ "(1004,'2014-01-05')," + "(1005,'2014-01-06')";

		try {
			conn = driver.getConnection(baseConf);
			stmt = conn.createStatement();
			stmt.execute(dropTables);
			stmt.execute(createFact);
			stmt.execute(createDim1);
			stmt.execute(createDim2);
			stmt.execute(createDim3);
			stmt.execute(createDim4);
			stmt.execute(insertFact);
			stmt.execute(insertDim1);
			stmt.execute(insertDim2);
			stmt.execute(insertDim3);
			stmt.execute(insertDim4);

		} finally {
			if (stmt != null) {
				stmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}

	@Test
	public void createSchema() throws Exception {
		createTables();
	}

	@Test
	public void testExecute1() throws Exception {
		testCreateJdbcDriver();
		String query = "select fact.day_key,account_dim_jdbc.managed_type,account_dim_jdbc.account_name, "
				+ "sum(fact.spend) spend, sum(fact.billed_clicks) billed_clicks "
				+ "from test_fact_jdbc fact inner join account_dim_jdbc account_dim_jdbc "
				+ "on fact.advertiser_account_key = account_dim_jdbc.account_key "
				+ "inner join date_dim_jdbc date_dim_jdbc on fact.day_key = date_dim_jdbc.day_key "
				+ "where date_dim_jdbc .full_date between '2014-01-01' and '2014-01-03' "
				+ "group by fact.day_key,account_dim_jdbc.managed_type,account_dim_jdbc.account_name "
				+ "order by sum(fact.spend) ";

		QueryContext context = new QueryContext(query, "root", baseConf);
		GrillResultSet resultSet = driver.execute(context);
		assertNotNull(resultSet);

		if (resultSet instanceof InMemoryResultSet) {
			InMemoryResultSet rs = (InMemoryResultSet) resultSet;
			GrillResultSetMetadata rsMeta = rs.getMetadata();
			assertEquals(rsMeta.getColumns().size(), 5);

			ResultColumn col1 = rsMeta.getColumns().get(0);
			assertEquals(col1.getType(), ResultColumnType.INT);
			assertEquals(col1.getName(), "day_key");

			ResultColumn col2 = rsMeta.getColumns().get(1);
			assertEquals(col2.getType(), ResultColumnType.STRING);
			assertEquals(col2.getName(), "managed_type");

			ResultColumn col3 = rsMeta.getColumns().get(2);
			assertEquals(col3.getType(), ResultColumnType.STRING);
			assertEquals(col3.getName(), "account_name");

			ResultColumn col4 = rsMeta.getColumns().get(3);
			assertEquals(col4.getType(), ResultColumnType.DOUBLE);
			assertEquals(col4.getName(), "spend");

			ResultColumn col5 = rsMeta.getColumns().get(4);
			assertEquals(col5.getType(), ResultColumnType.DECIMAL);
			assertEquals(col5.getName(), "billed_clicks");

			while (rs.hasNext()) {
				ResultRow row = rs.next();
				List<Object> rowObjects = row.getValues();
				System.out.println(rowObjects);
			}

			if (rs instanceof JDBCResultSet) {
				((JDBCResultSet) rs).close();
			}
		}
	}

	@Test
	public void testExecute2() throws Exception {
		testCreateJdbcDriver();
		String query = "select date_dim_jdbc.full_date,account_dim_jdbc.managed_type,account_dim_jdbc.account_name, "
				+ "sum(fact.spend) spend, sum(fact.billed_clicks) billed_clicks "
				+ "from test_fact_jdbc fact inner join account_dim_jdbc account_dim_jdbc "
				+ "on fact.advertiser_account_key = account_dim_jdbc.account_key  "
				+ "inner join date_dim_jdbc date_dim_jdbc on fact.day_key = date_dim_jdbc.day_key "
				+ "left outer join country_dim_jdbc  country_dim_jdbc on fact.served_country_key = country_dim_jdbc.country_key "
				+ "and country_dim_jdbc.country_name = 'USA' "
				+ "right outer join operator_dim_jdbc operator_dim_jdbc on fact.operator_key = operator_dim_jdbc.operator_key "
				+ "and operator_dim_jdbc.operator_name not in ('Airtel','MTS')"
				+ "where date_dim_jdbc .full_date between '2014-01-01' and '2014-01-05' "
				+ "and account_dim_jdbc.account_name = 'morrison software' "
				+ "group by fact.day_key,account_dim_jdbc.managed_type,account_dim_jdbc.account_name "
				+ "order by sum(fact.spend) ";

		QueryContext context = new QueryContext(query, "user1", baseConf);
		GrillResultSet resultSet = driver.execute(context);
		assertNotNull(resultSet);

		if (resultSet instanceof InMemoryResultSet) {
			InMemoryResultSet rs = (InMemoryResultSet) resultSet;
			GrillResultSetMetadata rsMeta = rs.getMetadata();
			assertEquals(rsMeta.getColumns().size(), 5);

			ResultColumn col1 = rsMeta.getColumns().get(0);
			assertEquals(col1.getType(), ResultColumnType.TIMESTAMP);
			assertEquals(col1.getName(), "full_date");

			ResultColumn col2 = rsMeta.getColumns().get(1);
			assertEquals(col2.getType(), ResultColumnType.STRING);
			assertEquals(col2.getName(), "managed_type");

			ResultColumn col3 = rsMeta.getColumns().get(2);
			assertEquals(col3.getType(), ResultColumnType.STRING);
			assertEquals(col3.getName(), "account_name");

			ResultColumn col4 = rsMeta.getColumns().get(3);
			assertEquals(col4.getType(), ResultColumnType.DOUBLE);
			assertEquals(col4.getName(), "spend");

			ResultColumn col5 = rsMeta.getColumns().get(4);
			assertEquals(col5.getType(), ResultColumnType.DECIMAL);
			assertEquals(col5.getName(), "billed_clicks");

			while (rs.hasNext()) {
				ResultRow row = rs.next();
				List<Object> rowObjects = row.getValues();
				System.out.println(rowObjects);
			}

			if (rs instanceof JDBCResultSet) {
				((JDBCResultSet) rs).close();
			}
		}
	}

}