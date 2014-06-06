package com.inmobi.grill.server.ml;

import com.inmobi.grill.api.GrillException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class TableTrainingSpec {
  public static final Log LOG = LogFactory.getLog(TableTrainingSpec.class);

  public static TableTrainingSpecBuilder newBuilder() {
    return new TableTrainingSpecBuilder();
  }


  public static class TableTrainingSpecBuilder {
    final TableTrainingSpec spec;

    public TableTrainingSpecBuilder() {
      spec = new TableTrainingSpec();
    }

    public TableTrainingSpecBuilder hiveConf(HiveConf conf) {
      spec.conf = conf;
      return this;
    }

    public TableTrainingSpecBuilder database(String db) {
      spec.db = db;
      return this;
    }

    public TableTrainingSpecBuilder table(String table) {
      spec.table = table;
      return this;
    }

    public TableTrainingSpecBuilder partitionFilter(String partFilter) {
      spec.partitionFilter = partFilter;
      return this;
    }

    public TableTrainingSpecBuilder labelColumn(String labelColumn) {
      spec.labelColumn = labelColumn;
      return this;
    }

    public TableTrainingSpecBuilder featureColumns(List<String> featureColumns) {
      spec.featureColumns = featureColumns;
      return this;
    }

    public TableTrainingSpec build() {
      return spec;
    }
  }

  private String db;
  private String table;
  private String partitionFilter;
  private List<String> featureColumns;
  private String labelColumn;
  private HiveConf conf;
  int labelPos;
  int[] featurePositions;
  int numFeatures;

  boolean validate() {
    List<FieldSchema> columns;
    try {
      Hive metastoreClient = Hive.get(conf);
      Table tbl = (db == null) ? metastoreClient.getTable(table) : metastoreClient.getTable(db, table);
      columns = tbl.getAllCols();
    } catch (HiveException exc) {
      LOG.error("Error getting table info " + toString(), exc);
      return false;
    }

    boolean valid = false;
    if (columns != null && !columns.isEmpty()) {
      // Check labeled column
      List<String> columnNames = new ArrayList<String>();
      for (FieldSchema col : columns) {
        columnNames.add(col.getName());
      }

      // Need at least one feature column and one label column
      valid = columnNames.contains(labelColumn) && columnNames.size() > 1;

      if (valid) {
        labelPos = columnNames.indexOf(labelColumn);

        // Check feature columns
        if (featureColumns == null || featureColumns.isEmpty()) {
          // feature columns are not provided, so all columns except label column are feature columns
          featurePositions = new int[columnNames.size() - 1];
          int p = 0;
          for (int i = 0 ; i < columnNames.size(); i++) {
            if (i == labelPos) {
              continue;
            }
            featurePositions[p++] = i;
          }

          columnNames.remove(labelPos);
          featureColumns = columnNames;
        } else {
          // Feature columns were provided, verify all feature columns are present in the table
          valid = columnNames.containsAll(featureColumns);
          if (valid) {
            // Get feature positions
            featurePositions = new int[featureColumns.size()];
            for (int i = 0; i < featureColumns.size(); i++) {
              featurePositions[i] = columnNames.indexOf(featureColumns.get(i));
            }
          }
        }
        numFeatures = featureColumns.size();
      }
    }

    return valid;
  }

  public RDD<LabeledPoint> createTrainableRDD(JavaSparkContext sparkContext) throws GrillException {
    // Validate the spec
    if (!validate()) {
      throw new GrillException("Table spec not valid: " + toString());
    }

    // Get the RDD for table
    JavaPairRDD<WritableComparable, HCatRecord> tableRDD;
    try {
      tableRDD = HiveTableRDD.createHiveTableRDD(sparkContext, conf, db, table, partitionFilter);
    } catch (IOException e) {
      throw new GrillException(e);
    }

    // Map into trainable RDD
    // TODO: Figure out a way to use custom value mappers
    FeatureValueMapper[] valueMappers = new FeatureValueMapper[numFeatures];
    final DoubleValueMapper doubleMapper = new DoubleValueMapper();
    for (int i = 0 ; i < numFeatures; i++) {
      valueMappers[i] = doubleMapper;
    }

    ColumnFeatureFunction trainPrepFunction =
      new ColumnFeatureFunction(featurePositions, valueMappers, labelPos, numFeatures, 0);

    return tableRDD.map(trainPrepFunction).rdd();
  }

  @Override
  public String toString() {
    return StringUtils.join(new String[]{db, table, partitionFilter, labelColumn}, ",");
  }
}
