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
package org.apache.lens.server.query.rewrite.dsl;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.query.rewrite.dsl.DSL;
import org.apache.hadoop.hive.conf.HiveConf;
import parquet.Preconditions;

import java.util.*;

public final class DSLRegistry {

  private static  DSLRegistry INSTANCE = new DSLRegistry();

  private DSLRegistry() {
  }

  private List<DSL> DSLs = new ArrayList<DSL>();

  private void register(DSL DSL) {
     DSLs.add(DSL);
  }

  public Collection<DSL> getDSLs() {
    return DSLs;
  }

  public void init(HiveConf conf) {
    final String[] DSLNames = conf.getStrings(LensConfConstants.QUERY_DSLS);
    if (DSLNames != null) {
      for (String DSLName : DSLNames) {
        try {
          final Class<?> dslClass = conf.getClass(LensConfConstants.DSL_QUERY_PFX + DSLName + LensConfConstants.DSL_IMPL_SUFFIX, null);
          if(dslClass == null) {
            throw new IllegalStateException("DSL class could not be loaded " + conf.get(LensConfConstants.DSL_QUERY_PFX + DSLName + LensConfConstants.DSL_IMPL_SUFFIX));
          }
          final DSL dslInstance = (DSL) dslClass.newInstance();
          register(dslInstance);
        } catch (InstantiationException e) {
          throw new IllegalStateException("DSL " + DSLName + " could not be loaded ", e);
        } catch (IllegalAccessException e) {
          throw new IllegalStateException("DSL " + DSLName + " could not be loaded ", e);
        }
      }
    }
  }

  public void init(HiveConf conf, ClassLoader classLoader) {
    final String[] DSLNames = conf.getStrings(LensConfConstants.QUERY_DSLS);
    if (DSLNames != null) {
      for (String DSLName : DSLNames) {
        try {
          final Class<?> dslClass = classLoader.loadClass(conf.get(LensConfConstants.DSL_QUERY_PFX + DSLName + LensConfConstants.DSL_IMPL_SUFFIX, null));
          if(dslClass == null) {
            throw new IllegalStateException("DSL class could not be loaded " + conf.get(LensConfConstants.DSL_QUERY_PFX + DSLName + LensConfConstants.DSL_IMPL_SUFFIX));
          }
          final DSL dslInstance = (DSL) dslClass.newInstance();
          register(dslInstance);
        } catch (InstantiationException e) {
          throw new IllegalStateException("DSL " + DSLName + " could not be loaded ", e);
        } catch (IllegalAccessException e) {
          throw new IllegalStateException("DSL " + DSLName + " could not be loaded ", e);
        } catch (ClassNotFoundException e) {
          throw new IllegalStateException("DSL " + DSLName + " could not be loaded ", e);
        }
      }
    }
  }

  public static DSLRegistry getInstance() {
    return INSTANCE;
  }
}
