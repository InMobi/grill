package com.inmobi.grill.ml;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Table specification for running test on a table
 */
public class TableTestingSpec {
  public static final Log LOG = LogFactory.getLog(TableTestingSpec.class);

  private String db;
  private String table;
  // TODO use partition condition
  private String partitionFilter;
  private List<String> featureColumns;
  private String labelColumn;
  private String outputColumn;
  private String outputTable;
  private transient HiveConf conf;
  private String algorithm;
  private String modelID;

  public static class TableTestingSpecBuilder {
    private final TableTestingSpec spec;

    public TableTestingSpecBuilder() {
      spec = new TableTestingSpec();
    }

    public TableTestingSpecBuilder database(String database) {
      spec.db = database;
      return this;
    }

    public TableTestingSpecBuilder table(String table) {
      spec.table = table;
      return this;
    }

    public TableTestingSpecBuilder partitionFilter(String partFilter) {
      spec.partitionFilter = partFilter;
      return this;
    }

    public TableTestingSpecBuilder featureColumns(List<String> featureColumns) {
      spec.featureColumns = featureColumns;
      return this;
    }

    public TableTestingSpecBuilder labeColumn(String labelColumn) {
      spec.labelColumn = labelColumn;
      return this;
    }

    public TableTestingSpecBuilder outputColumn(String outputColumn) {
      spec.outputColumn = outputColumn;
      return this;
    }

    public TableTestingSpecBuilder outputTable(String table) {
      spec.outputTable = table;
      return this;
    }

    public TableTestingSpecBuilder hiveConf(HiveConf conf) {
      spec.conf = conf;
      return this;
    }

    public TableTestingSpecBuilder algorithm(String algorithm) {
      spec.algorithm = algorithm;
      return this;
    }

    public TableTestingSpecBuilder modelID(String modelID) {
      spec.modelID = modelID;
      return this;
    }

    public TableTestingSpec build() {
      return spec;
    }
  }

  public static TableTestingSpecBuilder newBuilder() {
    return new TableTestingSpecBuilder();
  }

  public boolean validate() {
    List<FieldSchema> columns;
    try {
      Hive metastoreClient = Hive.get(conf);
      Table tbl = (db == null) ? metastoreClient.getTable(table) : metastoreClient.getTable(db, table);
      columns = tbl.getAllCols();
    } catch (HiveException exc) {
      LOG.error("Error getting table info " + toString(), exc);
      return false;
    }

    // Check if labeled column and feature columns are contained in the table
    List<String> testTableColumns = new ArrayList<String>(columns.size());
    for (FieldSchema column : columns) {
      testTableColumns.add(column.getName());
    }

    if (!testTableColumns.containsAll(featureColumns)) {
      LOG.info("Invalid feature columns: " + featureColumns + ". Actual columns in table:" + testTableColumns);
      return false;
    }

    if (!testTableColumns.contains(labelColumn)) {
      LOG.info("Invalid label column: " + labelColumn + ". Actual columns in table:" + testTableColumns);
      return false;
    }

    if (StringUtils.isBlank(outputColumn)) {
      LOG.info("Output column is required");
      return false;
    }

    if (StringUtils.isBlank(outputTable)) {
      LOG.info("Output table is required");
      return false;
    }
    return true;
  }

  public String getTestQuery() {
    if (!validate()) {
      return null;
    }

    StringBuilder q = new StringBuilder("CREATE TABLE " + outputTable + " AS SELECT ");
    String featureCols = StringUtils.join(featureColumns, ",");
    q.append(featureCols)
      .append(",")
      .append(labelColumn)
      .append(", ")
      .append("predict(")
      .append("'").append(algorithm).append("', ")
      .append("'").append(modelID).append("', ")
      .append(featureCols).append(") ").append(outputColumn)
      .append(" FROM ")
      .append(table);

    return q.toString();
  }
}
