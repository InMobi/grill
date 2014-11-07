package org.apache.lens.server.api.driver;

/*
 * #%L
 * Lens API for server and extensions
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;
import org.apache.lens.api.query.QueryResultSetMetadata;
import org.apache.lens.api.query.ResultColumn;


public abstract class LensResultSetMetadata {

  public abstract List<ColumnDescriptor> getColumns();

  public QueryResultSetMetadata toQueryResultSetMetadata() {
    List<ResultColumn> result = new ArrayList<ResultColumn>();
    for (ColumnDescriptor col : getColumns()) {
      result.add(new ResultColumn(col.getName(), col.getType().getName()));
    }
    return new QueryResultSetMetadata(result);
  }

  public static String getQualifiedTypeName(TypeDescriptor typeDesc) {
    if (typeDesc.getType().isQualifiedType()) {
      switch (typeDesc.getType()) {
      case VARCHAR_TYPE :
        return VarcharTypeInfo.getQualifiedName(typeDesc.getTypeName(),
            typeDesc.getTypeQualifiers().getCharacterMaximumLength()).toLowerCase();
      case CHAR_TYPE :
        return CharTypeInfo.getQualifiedName(typeDesc.getTypeName(),
            typeDesc.getTypeQualifiers().getCharacterMaximumLength()).toLowerCase();
      case DECIMAL_TYPE :
        return DecimalTypeInfo.getQualifiedName(
            typeDesc.getTypeQualifiers().getPrecision(),
            typeDesc.getTypeQualifiers().getScale()).toLowerCase();
      }
    }
    return typeDesc.getTypeName().toLowerCase();
  }
}
