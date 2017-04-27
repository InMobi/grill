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

import org.apache.hadoop.hive.ql.metadata.Table;

import lombok.Getter;

public class CubeVirtualFactTable extends CubeFactTable {

  @Getter
  private CubeFactTable sourceCubeFactTable;

  public CubeVirtualFactTable(Table hiveTable, Table sourceHiveTable) {
    super(hiveTable);
    this.sourceCubeFactTable = new CubeFactTable(sourceHiveTable);
  }

  public CubeVirtualFactTable(String cubeName, String factName, double weight, Map<String, String> properties,
    CubeFactTable sourceFact) {
    super(cubeName, factName, sourceFact.getColumns(), sourceFact.getUpdatePeriods(), weight, properties);
    this.sourceCubeFactTable = sourceFact;
  }

  public CubeVirtualFactTable(String cubeName, String factName, Map<String, String> properties,
    CubeFactTable sourceFact) {
    super(cubeName, factName, sourceFact.getColumns(), sourceFact.getUpdatePeriods(), sourceFact.weight(), properties);
    this.sourceCubeFactTable = sourceFact;
  }

  public CubeVirtualFactTable(String cubeName, String factName, CubeFactTable sourceFact) {
    super(cubeName, factName, sourceFact.getColumns(), sourceFact.getUpdatePeriods(), sourceFact.weight(),
      new HashMap<String, String>());
    this.sourceCubeFactTable = sourceFact;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.VIRTUAL_FACT;
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
  public String getDataCompletenessTag() {
    return this.sourceCubeFactTable.getDataCompletenessTag();
  }

}
