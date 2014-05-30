package com.inmobi.grill.server.ml;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;

/**
 * Generic UDF to laod ML Models saved in HDFS and apply the model on list of columns passed as
 * argument
 */
@Description(
  name = "ml_apply_model",
  value = "_FUNC_(str, ...) - Call Yoda UDF that returns value of String type")
public class HiveMLUDF extends UDF {
   private Path modelPath;

  /**
   * @param path location of serialized model on Hadoop compatible FS
   * @param features list of values of features in the order expected by the model
   * @return model output
   */
  public double evaluate(String path, Object ... features) {
    MLModel model = null;
    try {
      if (modelPath == null) {
        modelPath = new Path(path);
      }

      model = ModelLoader.loadModel(modelPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return model.predict(features);
  }
}
