/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.metadata;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

/**
 * The Fact table interface
 */
public interface FactTable extends Named {

  /**
   * Get map of storage to update period mapping
   *
   * @return Map of storage to set of update periods
   */
  public Map<String, Set<UpdatePeriod>> getUpdatePeriods();

  /**
   * Cube to which this fact belongs to
   *
   * @return the cube string
   */
  public String getCubeName();

  /**
   * The set of Storage names
   *
   * @return set of strings
   */
  public Set<String> getStorages();

  /**
   *The type of the fact
   *
   * @return table type {@link CubeTableType}
   */
  public CubeTableType getTableType();

  /**
   * Config properties
   *
   * @return map of string, string
   */
  public Map<String, String> getProperties();

  /**
   * Valid columns of the fact
   *
   * @return list of column names
   */
  public Set<String> getValidColumns();

  /**
   * Weight of the fact
   *
   * @return weight of the fact in double
   */
  public double weight();

  /**
   * Set of all the columns names of the fact
   *
   * @return set of column names
   */
  public Set<String> getAllFieldNames();

  /**
   *tag for checking data completeness
   *
   * @return Tag String
   */
  public String getDataCompletenessTag();

  /**
   * List of columns of the fact
   *
   * @return set of {@link FieldSchema}
   */
  public List<FieldSchema> getColumns();

  /**
   * Is Aggregated Fact
   *
   * @return true if fact is Aggregated , false otherwise
   */
  public boolean isAggregated();

  /**
   * Absolute start time of the fact
   *
   * @return Absolute Start time of the fact {@link Date}
   */
  public Date getAbsoluteStartTime();

  /**
   * Relative start time of the fact
   *
   * @return Relative Start time of the fact {@link Date}
   */
  public Date getRelativeStartTime();

  /**
   * Start time of the fact
   *
   * @return Start time of the fact {@link Date}
   */
  public Date getStartTime();

  /**
   * Absolute end time of the fact
   *
   * @return Absolute End time of the fact {@link Date}
   */
  public Date getAbsoluteEndTime();

  /**
   * Relative End time of the Fact
   *
   * @return Relative end time of the fact {@link Date}
   */
  public Date getRelativeEndTime();

  /**
   * End time of the fact
   *
   * @return End time of the fact {@link Date}
   */
  public Date getEndTime();

  /**
   * Is Virtual Fact
   *
   * @return true if fact is a virtual fact, false otherwise
   */
  public boolean isVirtualFact();

  /**
   * Storage name of the fact
   *
   * @return Storage name of the fact
   */
  public String getSourceFactName();

}
