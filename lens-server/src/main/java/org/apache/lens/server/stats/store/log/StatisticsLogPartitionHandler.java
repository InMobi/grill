package org.apache.lens.server.stats.store.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.IOUtils;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.stats.event.LoggableLensStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class used to copy log files to HDFS and add partition to hive metastore.
 */
public class StatisticsLogPartitionHandler extends AsyncEventListener<PartitionEvent> {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsLogPartitionHandler.class);

  /** The Constant LOG_PARTITION_HANDLER_COUNTER. */
  public static final String LOG_PARTITION_HANDLER_COUNTER = "log-partition-handler-errors";

  /** The warehouse path. */
  private Path warehousePath;

  /** The client. */
  private Hive client;

  /** The database. */
  private String database;

  /**
   * Initialize.
   *
   * @param conf
   *          the conf
   */
  public void initialize(Configuration conf) {
    String temp = conf.get(LensConfConstants.STATISTICS_WAREHOUSE_KEY, LensConfConstants.DEFAULT_STATISTICS_WAREHOUSE);
    warehousePath = new Path(temp);
    database = conf.get(LensConfConstants.STATISTICS_DATABASE_KEY, LensConfConstants.DEFAULT_STATISTICS_DATABASE);
    try {
      client = Hive.get();
    } catch (Exception e) {
      LOG.error("Unable to connect to hive metastore", e);
      throw new IllegalArgumentException("Unable to connect to hive metastore", e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
   */
  @Override
  public void process(PartitionEvent event) {
    String eventName = event.getEventName();
    Map<String, String> partitionMap = event.getPartMap();
    Path eventDir = new Path(warehousePath, eventName);
    for (Map.Entry<String, String> entry : partitionMap.entrySet()) {
      String partitionPath = entry.getKey().replace("-", "/");
      Path finalPath = new Path(eventDir, partitionPath + "/" + eventName + ".log");
      try {
        copyToHdfs(entry.getValue(), finalPath);
        boolean added = addPartition(eventName, entry.getKey(), finalPath, event.getClassName());
        if (added) {
          new File(entry.getValue()).delete();
        }
      } catch (Exception e) {
        MetricsService svc = (MetricsService) LensServices.get().getService(MetricsService.NAME);
        svc.incrCounter(StatisticsLogPartitionHandler.class, LOG_PARTITION_HANDLER_COUNTER);
        LOG.error("Unable to copy file to the file system", e);
      }
    }
  }

  /**
   * Adds the partition.
   *
   * @param eventName
   *          the event name
   * @param key
   *          the key
   * @param finalPath
   *          the final path
   * @param className
   *          the class name
   * @return true, if successful
   */
  private boolean addPartition(String eventName, String key, Path finalPath, String className) {

    try {
      Table t = getTable(eventName, className);
      HashMap<String, String> partSpec = new HashMap<String, String>();
      partSpec.put("dt", key);
      Partition p = client.createPartition(t, partSpec);
      p.setLocation(finalPath.toString());
      client.alterPartition(database, eventName, p);
      return true;
    } catch (Exception e) {
      LOG.warn("Unable to add the partition ", e);
      return false;
    }
  }

  /**
   * Gets the table.
   *
   * @param eventName
   *          the event name
   * @param className
   *          the class name
   * @return the table
   * @throws Exception
   *           the exception
   */
  private Table getTable(String eventName, String className) throws Exception {
    Table tmp = null;
    try {
      tmp = client.getTable(database, eventName, false);
      if (tmp == null) {
        tmp = createTable(eventName, className);
      }
    } catch (HiveException e) {
      LOG.warn("Exception thrown while creating the table", e);
    }
    return tmp;
  }

  /**
   * Creates the table.
   *
   * @param eventName
   *          the event name
   * @param className
   *          the class name
   * @return the table
   * @throws Exception
   *           the exception
   */
  private Table createTable(String eventName, String className) throws Exception {
    Table tmp;
    try {
      Database db = new Database();
      db.setName(database);
      client.createDatabase(db, true);
      Class<LoggableLensStatistics> statisticsClass = (Class<LoggableLensStatistics>) Class.forName(className);
      LoggableLensStatistics stat = statisticsClass.newInstance();
      Configuration conf = new Configuration();
      conf.addResource("hive-site.xml");
      tmp = stat.getHiveTable(conf);
      tmp.setDbName(database);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating table  " + tmp.getTableName());
      }
      client.createTable(tmp);
      tmp = client.getTable(database, eventName);
    } catch (Exception e1) {
      LOG.warn("Unable to create hive table, exiting", e1);
      throw e1;
    }
    return tmp;
  }

  /**
   * Copy to hdfs.
   *
   * @param localPath
   *          the local path
   * @param finalPath
   *          the final path
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void copyToHdfs(String localPath, Path finalPath) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = finalPath.getFileSystem(conf);
    if (fs.exists(finalPath)) {
      fs.delete(finalPath, true);
    }
    IOUtils.copyBytes(new FileInputStream(localPath), fs.create(finalPath), conf, true);
  }
}
