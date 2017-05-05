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
 * Created by rajithar on 3/5/17.
 */
public interface FactTable extends Named {

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
