/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.metadata;

import lombok.Data;

@Data
public class TableReference {
  private final String destTable;
  private final String destColumn;
  private boolean mapsToMany = false;

  public TableReference(String destTable, String destColumn) {
    this(destTable, destColumn, false);
  }
  public TableReference(String destTable, String destColumn, boolean mapsToMany) {
    this.destTable = destTable.toLowerCase();
    this.destColumn = destColumn.toLowerCase();
    this.mapsToMany = mapsToMany;
  }
  public TableReference(String reference) {
    String[] desttoks = reference.split("\\.+");
    this.destTable = desttoks[0];
    this.destColumn = desttoks[1];
    if (desttoks.length > 2) {
      this.mapsToMany = Boolean.parseBoolean(desttoks[2]);
    }
  }
  @Override
  public String toString() {
    return destTable + "." + destColumn + (mapsToMany ? "[n]" : "");
  }

}
