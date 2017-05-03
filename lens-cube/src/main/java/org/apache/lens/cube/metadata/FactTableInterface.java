package org.apache.lens.cube.metadata;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

/**
 * Created by rajithar on 3/5/17.
 */
public interface FactTableInterface extends Named{

  public Map<String, Set<UpdatePeriod>> getUpdatePeriods();

  public String getCubeName();

  public Set<String> getStorages();

  public CubeTableType getTableType();

  public Map<String, String> getProperties();

  public List<String> getValidColumns();

  public double weight();

  public Set<String> getAllFieldNames();

  public String getDataCompletenessTag();

  public List<FieldSchema> getColumns();

  public boolean isAggregated();

  public Date getDateFromProperty(String propKey, boolean relative, boolean start);
}
