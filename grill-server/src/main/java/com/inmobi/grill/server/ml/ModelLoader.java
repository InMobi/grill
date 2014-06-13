package com.inmobi.grill.server.ml;

import com.inmobi.grill.server.api.ml.MLModel;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Load ML models from a FS location
 */
public class ModelLoader {
  public static final String MODEL_PATH_BASE_DIR = "grill.ml.model.basedir";
  public static final String MODEL_PATH_BASE_DIR_DEFAULT = "file:///tmp";

  public static final Log LOG = LogFactory.getLog(ModelLoader.class);
  private static Map<Path, MLModel> modelCache = new HashMap<Path, MLModel>();

  public static Path getModelLocation(Configuration conf, String algorithm, String modelID) {
    String modelDataBaseDir = conf.get(MODEL_PATH_BASE_DIR, MODEL_PATH_BASE_DIR_DEFAULT);
    // Model location format - <modelDataBaseDir>/<algorithm>/modelID
    return new Path(new Path(new Path(modelDataBaseDir), algorithm), modelID);
  }

  public static MLModel loadModel(JobConf conf, String algorithm, String modelID) throws IOException {
    LOG.info("Loading model algorithm: " + algorithm + " modelID: " + modelID);

    Path modelPath = getModelLocation(conf, algorithm, modelID);

    if (modelCache.containsKey(modelPath)) {
      return modelCache.get(modelPath);
    }

    FileSystem fs = modelPath.getFileSystem(new HiveConf());

    if (!fs.exists(modelPath)) {
      return null;
    }

    ObjectInputStream ois = null;

    try {
      ois = new ObjectInputStream(fs.open(modelPath));
      MLModel model = (MLModel) ois.readObject();
      modelCache.put(modelPath, model);
      LOG.info("Loaded model " + model.getId() + " from location " + modelPath);
      return model;
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(ois);
    }
  }

  protected static void clearCache() {
    modelCache.clear();
  }

}
