package com.inmobi.grill.server.query.rewrite.dsl;

/*
 * #%L
 * Grill Cube Driver
 * %%
 * Copyright (C) 2014 Inmobi
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
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.query.rewrite.dsl.DSL;
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
    final String[] DSLNames = conf.getStrings(GrillConfConstants.GRILL_QUERY_DSLS);
    if (DSLNames != null) {
      for (String DSLName : DSLNames) {
        try {
          final Class<?> dslClass = conf.getClass(GrillConfConstants.DSL_QUERY_PFX + DSLName + GrillConfConstants.DSL_IMPL_SUFFIX, null);
          if(dslClass == null) {
            throw new IllegalStateException("DSL class could not be loaded " + conf.get(GrillConfConstants.DSL_QUERY_PFX + DSLName + GrillConfConstants.DSL_IMPL_SUFFIX));
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

  public static DSLRegistry getInstance() {
    return INSTANCE;
  }
}
