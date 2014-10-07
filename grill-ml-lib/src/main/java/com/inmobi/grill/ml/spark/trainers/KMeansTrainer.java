package com.inmobi.grill.ml.spark.trainers;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.ml.*;
import com.inmobi.grill.ml.spark.HiveTableRDD;
import com.inmobi.grill.ml.spark.models.KMeansClusteringModel;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.List;

@Algorithm(
  name = "spark_kmeans_trainer",
  description = "Spark MLLib KMeans trainer"
)
public class KMeansTrainer implements MLTrainer {
  private transient GrillConf conf;
  private JavaSparkContext sparkContext;

  @TrainerParam(name = "partition", help = "Partition filter to be used while constructing table RDD")
  private String partFilter = null;

  @TrainerParam(name = "k", help = "Number of cluster")
  private int k;

  @TrainerParam(name ="maxIterations", help = "Maximum number of iterations",
  defaultValue = "100")
  private int maxIterations = 100;

  @TrainerParam(name = "runs", help = "Number of parallel run",
  defaultValue = "1")
  private int runs = 1;

  @TrainerParam(name = "initializationMode", help = "initialization model, either \"random\" or \"k-means||\" (default).",
  defaultValue = "k-means||")
  private String initializationMode = "k-means||";

  @Override
  public String getName() {
    return getClass().getAnnotation(Algorithm.class).name();
  }

  @Override
  public String getDescription() {
    return getClass().getAnnotation(Algorithm.class).description();
  }

  @Override
  public void configure(GrillConf configuration) {
    this.conf = configuration;
  }

  @Override
  public GrillConf getConf() {
    return conf;
  }

  @Override
  public MLModel train(GrillConf conf, String db, String table, String modelId, String... params)
    throws GrillException {
    List<String> features = TrainerArgParser.parseArgs(this, params);
    final int featurePositions[] = new int[features.size()];
    final int NUM_FEATURES = features.size();

    JavaPairRDD<WritableComparable, HCatRecord> rdd = null;
    try {
      // Map feature names to positions
      Table tbl = Hive.get(toHiveConf(conf)).getTable(db, table);
      List<FieldSchema> allCols = tbl.getAllCols();
      int f = 0;
      for (int i = 0; i < tbl.getAllCols().size(); i++) {
        String colName = allCols.get(i).getName();
        if (features.contains(colName)) {
          featurePositions[f++] = i;
        }
      }

      rdd = HiveTableRDD.createHiveTableRDD(sparkContext,
        toHiveConf(conf), db, table, partFilter);
      JavaRDD<Vector> trainableRDD = rdd.map(new Function<Tuple2<WritableComparable, HCatRecord>, Vector>() {
        @Override
        public Vector call(Tuple2<WritableComparable, HCatRecord> v1) throws Exception {
          HCatRecord hCatRecord = v1._2();
          double arr[] = new double[NUM_FEATURES];
          for (int i = 0; i < NUM_FEATURES; i++) {
            Object val =  hCatRecord.get(featurePositions[i]);
            arr[i] = val == null ? 0d : (Double) val;
          }
          return Vectors.dense(arr);
        }
      });

      KMeansModel model =
        KMeans.train(trainableRDD.rdd(), k, maxIterations, runs, initializationMode);
      return new KMeansClusteringModel(modelId, model);
    } catch (Exception e) {
      throw new GrillException("KMeans trainer failed for " + db + "." + table, e);
    }
  }

  private HiveConf toHiveConf(GrillConf conf) {
    HiveConf hiveConf = new HiveConf();
    for (String key : conf.getProperties().keySet()) {
      hiveConf.set(key, conf.getProperties().get(key));
    }
    return hiveConf;
  }
}
