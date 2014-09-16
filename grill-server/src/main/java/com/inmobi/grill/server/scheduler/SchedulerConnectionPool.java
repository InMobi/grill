package com.inmobi.grill.server.scheduler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

/** 
 * Conncetion Pool class for scheduler db
 * @author prashant.gupta
 *
 */
public class SchedulerConnectionPool {
  private static final Logger LOGGER = Logger
      .getLogger(SchedulerConnectionPool.class.getName());
  private static GenericObjectPool objectPool = new GenericObjectPool(null, 50,
      GenericObjectPool.WHEN_EXHAUSTED_GROW, 5000, true, false);
  private static ConnectionFactory connectionFactory =
      new DriverManagerConnectionFactory("jdbc:postgresql://"
          + SchedulerProps.get(SchedulerProps.PropNames.SCHEDULER_DB_URL),
          SchedulerProps.get(SchedulerProps.PropNames.SCHEDULER_DB_USER),
          SchedulerProps.get(SchedulerProps.PropNames.SCHEDULER_DB_PASS));

  // Create a test table in the scheduler db so that the below action succeeds.
  @SuppressWarnings("unused")
  private PoolableConnectionFactory factory = new PoolableConnectionFactory(
      connectionFactory, objectPool, null, "select * from test", false, true);
  private PoolingDataSource dataSource = new PoolingDataSource(objectPool);
  private AtomicBoolean alive = new AtomicBoolean(true);
  private static SchedulerConnectionPool instance = null;

  private SchedulerConnectionPool() {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      LOGGER.error(e);
    }
  }

  public static Connection getDataSource() {
    if (instance == null) {
      synchronized (SchedulerConnectionPool.class) {
        if (instance == null) {
          instance = new SchedulerConnectionPool();
        }
      }
    }
    Connection conn = null;
    if (instance.alive.get()) {
      try {
        conn = instance.dataSource.getConnection();
      } catch (SQLException e) {
        LOGGER.error(e);
      }
    }
    if (conn == null) {
      throw new RuntimeException("Failed to initialize conn in getdatasource");
    }
    return conn;
  }

  public static void shutdown() {
    if (instance == null) {
      instance = new SchedulerConnectionPool();
    }
    instance.alive.set(false);
  }

  public static void wakeup() {
    if (instance == null) {
      instance = new SchedulerConnectionPool();
    }
    instance.alive.set(true);
  }

}
