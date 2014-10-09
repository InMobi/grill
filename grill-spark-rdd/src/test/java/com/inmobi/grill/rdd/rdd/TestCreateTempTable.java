package com.inmobi.grill.rdd.rdd;


import com.inmobi.grill.api.query.QueryResultSetMetadata;
import com.inmobi.grill.api.query.ResultColumn;
import com.inmobi.grill.api.query.ResultColumnType;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(groups = {"unit"})
public class TestCreateTempTable {

  private JavaSparkContext jsc;

  @BeforeClass
  public void setup() throws Exception {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName("grill_rdd_test");
    sparkConf.setMaster("local");
    jsc = new JavaSparkContext(sparkConf);
  }

  @AfterClass
  public void destroy() throws Exception {
    if (jsc != null) {
      jsc.stop();
    }
  }

  @Test
  public void testCreateTempTable() throws Exception {
    GrillRDDClient rddClient = new GrillRDDClient(jsc);

    // Create sample data file
    File dataDir = new File("target/test-partition-data-dir");
    dataDir.mkdir();
    File dataFile = new File(dataDir, "sample-data-file.txt");
    dataFile.deleteOnExit();

    PrintWriter out = null;
    try {
      out = new PrintWriter(new FileOutputStream(dataFile));
      for (int i = 0; i < 10; i ++) {
        out.println("col"+i + "," + i);
      }
      out.flush();
    } finally {
      IOUtils.closeQuietly(out);
    }

    // Table schema
    ArrayList<ResultColumn> columns = new ArrayList<ResultColumn>();

    columns.add(new ResultColumn("name", ResultColumnType.STRING));
    columns.add(new ResultColumn("value", ResultColumnType.INT));

    QueryResultSetMetadata metadata = new QueryResultSetMetadata(columns);
    Hive metastoreClient = Hive.get(GrillRDDClient.hiveConf);
    String tableName = null;
    try {
      tableName = rddClient.createTempMetastoreTable(dataDir.toURI().toString(), metadata);
      assertNotNull(tableName);

      Table tbl = metastoreClient.getTable("default", tableName);
      assertNotNull(tbl);
      assertEquals(tbl.getTableName(), tableName);
      List<FieldSchema> partCols = tbl.getPartCols();
      assertNotNull(partCols);
      assertEquals(partCols.size(), 1);

      List<FieldSchema> tableColumns = tbl.getAllCols();
      assertEquals(tableColumns.size(), 3);

      Set<Partition> partitions = metastoreClient.getAllPartitionsOf(tbl);
      assertEquals(partitions.size(), 1);

      Partition partition = partitions.iterator().next();
      assertEquals(partition.getLocation() + "/", dataDir.toURI().toString());
    } finally {
      if (tableName != null) {
        try {
          metastoreClient.dropTable("default", tableName);
        } catch (Exception exc) {
          exc.printStackTrace();
        }
      }
    }

  }

}
