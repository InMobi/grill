package com.inmobi.grill.ml;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Generic UDF to laod ML Models saved in HDFS and apply the model on list of columns passed as
 * argument
 */
@Description(
  name = "predict",
  value = "_FUNC_(algorithm, modelID, features...) - Run prediction algorithm with given " +
    "algorithm name, model ID and input feature columns")
public class HiveMLUDF extends GenericUDF {

  public static final String UDF_NAME = "predict";
  public static final Log LOG = LogFactory.getLog(HiveMLUDF.class);
  private JobConf conf;
  private StringObjectInspector soi;
  private LazyDoubleObjectInspector doi;
  private MLModel model;

  /**
   * Currently we only support double as the return value
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
    // We require algo name, model id and at least one feature
    if (objectInspectors.length < 3) {
      throw new UDFArgumentLengthException("Algo name, model ID and at least one feature should be passed to "
        + UDF_NAME);
    }
    LOG.info(UDF_NAME + " initialized");
    return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    String algorithm = soi.getPrimitiveJavaObject(deferredObjects[0].get());
    String modelId = soi.getPrimitiveJavaObject(deferredObjects[1].get());

    Double features[] = new Double[deferredObjects.length - 2];
    for (int i = 2; i < deferredObjects.length; i++) {
      LazyDouble lazyDouble = (LazyDouble) deferredObjects[i].get();
      features[i - 2] = (lazyDouble == null) ? 0d : doi.get(lazyDouble);
    }

    try {
      if (model == null) {
        model = ModelLoader.loadModel(conf, algorithm, modelId);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }

    return model.predict(features);
  }

  @Override
  public String getDisplayString(String[] strings) {
    return UDF_NAME;
  }

  @Override
  public void configure(MapredContext context) {
    super.configure(context);
    conf = context.getJobConf();
    soi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    doi = LazyPrimitiveObjectInspectorFactory.LAZY_DOUBLE_OBJECT_INSPECTOR;
    LOG.info(UDF_NAME + " configured. Model base dir path: " + conf.get(ModelLoader.MODEL_PATH_BASE_DIR));
  }
}
