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

import java.util.*;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;

import lombok.Getter;

public class CubeVirtualFactTable extends AbstractCubeTable implements FactTableInterface{

  @Getter
  private CubeFactTable sourceCubeFactTable;
  private String cubeName;

  public CubeVirtualFactTable(Table hiveTable , Table sourceHiveTable) {
    super(hiveTable);
    this.cubeName = getFactCubeName(getName(), getProperties());
    this.sourceCubeFactTable = new CubeFactTable(sourceHiveTable);
  }

  public CubeVirtualFactTable(String cubeName, String virtualFactName, double weight, Map<String, String> properties,
    CubeFactTable sourceFact) {
    this(cubeName, virtualFactName, sourceFact.getColumns(), weight, properties);
    this.sourceCubeFactTable = sourceFact;
  }

  public CubeVirtualFactTable(String cubeName, String virtualFactName, Map<String, String> properties,
    CubeFactTable sourceFact) {
    this(cubeName, virtualFactName, sourceFact.getColumns(), sourceFact.weight(), properties);
    this.sourceCubeFactTable = sourceFact;
  }

  public CubeVirtualFactTable(String cubeName, String virtualFactName, CubeFactTable sourceFact) {
    this(cubeName, virtualFactName, sourceFact.getColumns(), sourceFact.weight(),
      new HashMap<String, String>());
    this.sourceCubeFactTable = sourceFact;
  }

  public CubeVirtualFactTable(String cubeName, String virtualFactName, List<FieldSchema> columns,
    double weight, Map<String, String> properties) {
    super(virtualFactName, columns, properties, weight);
    this.cubeName = cubeName;
    addProperties();
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    addCubeNames(getName(), getProperties(), cubeName);
    addSourceFactName(getName(), getProperties(), cubeName);
  }

  public static String getSourceFactName(String factName, Map<String, String> props) {
    return props.get(MetastoreUtil.getSourceFactNameKey(factName));
  }

  protected static void addSourceFactName(String factName, Map<String, String> props, String sourceFactName) {
    props.put(MetastoreUtil.getSourceFactNameKey(factName), sourceFactName);
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.VIRTUAL_FACT;
  }

  @Override
  public List<String> getValidColumns() {
    return this.sourceCubeFactTable.getValidColumns();
  }

  @Override
  public Set<String> getStorages() {
    return this.sourceCubeFactTable.getStorages();
  }

  @Override
  public Map<String, Set<UpdatePeriod>> getUpdatePeriods() {
    return this.sourceCubeFactTable.getUpdatePeriods();
  }

  @Override
  public String getCubeName() {
    return this.cubeName;
  }

  @Override
  public String getDataCompletenessTag() {
    return this.sourceCubeFactTable.getDataCompletenessTag();
  }

  @Override
  public boolean isAggregated() {
    return this.sourceCubeFactTable.isAggregated();
  }

}
