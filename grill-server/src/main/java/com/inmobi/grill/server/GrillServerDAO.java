package com.inmobi.grill.server;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.server.api.GrillConfConstants;

/**
 * Top level class which logs and retrieves data from Database
 */
public class GrillServerDAO {

  public DataSource ds;
  public void init(Configuration conf) {
    String className =
        conf.get(GrillConfConstants.GRILL_SERVER_DB_DRIVER_NAME,
            GrillConfConstants.DEFAULT_SERVER_DB_DRIVER_NAME);
    String jdbcUrl =
        conf.get(GrillConfConstants.GRILL_SERVER_DB_JDBC_URL,
            GrillConfConstants.DEFAULT_SERVER_DB_JDBC_URL);
    String userName =
        conf.get(GrillConfConstants.GRILL_SERVER_DB_JDBC_USER,
            GrillConfConstants.DEFAULT_SERVER_DB_USER);
    String pass =
        conf.get(GrillConfConstants.GRILL_SERVER_DB_JDBC_PASS,
            GrillConfConstants.DEFAULT_SERVER_DB_PASS);
    BasicDataSource tmp = new BasicDataSource();
    tmp.setDriverClassName(className);
    tmp.setUrl(jdbcUrl);
    tmp.setUsername(userName);
    tmp.setPassword(pass);
    ds = tmp;
  }

  public Connection getConnection() throws SQLException {
    return ds.getConnection();
  }

  public void createTable(String sql) throws SQLException {
    QueryRunner runner = new QueryRunner(ds);
    runner.update(sql);
  }

  public void dropTable(String sql) throws SQLException {
    QueryRunner runner = new QueryRunner(ds);
    runner.update(sql);
  }

}
