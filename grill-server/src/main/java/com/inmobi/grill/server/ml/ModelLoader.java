package com.inmobi.grill.server.ml;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Load ML models from a FS location
 */
public class ModelLoader {
  public static final Log LOG = LogFactory.getLog(ModelLoader.class);
  private static Map<Path, MLModel> modelCache = new HashMap<Path, MLModel>();

  public static MLModel loadModel(Path modelPath) throws IOException {
    if (modelCache.containsKey(modelPath)) {
      return modelCache.get(modelPath);
    }

    FileSystem fs = modelPath.getFileSystem(new HiveConf());
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
}
