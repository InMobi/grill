package org.apache.lens.server;

/*
 * #%L
 * Grill Server
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.api.GrillConfConstants;
import org.apache.lens.server.metastore.MetastoreResource;
import org.apache.lens.server.query.QueryServiceResource;
import org.apache.lens.server.quota.QuotaResource;
import org.apache.lens.server.scheduler.ScheduleResource;
import org.apache.lens.server.session.SessionResource;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

@ApplicationPath("/")
public class GrillApplication extends Application {

  public static final Log LOG = LogFactory.getLog(GrillApplication.class);
  public static HiveConf conf = GrillServerConf.get();

  @Override
  public Set<Class<?>> getClasses() {

    final Set<Class<?>> classes = new HashSet<Class<?>>();

    String[] resourceNames = conf.getStrings(GrillConfConstants.WS_RESOURCE_NAMES);
    String[] featureNames = conf.getStrings(GrillConfConstants.WS_FEATURE_NAMES);
    String[] listenerNames = conf.getStrings(GrillConfConstants.WS_LISTENER_NAMES);
    String[] filterNames = conf.getStrings(GrillConfConstants.WS_FILTER_NAMES);

    // register root resource
    for(String rName : resourceNames) {
      Class wsResourceClass = conf.getClass(
          GrillConfConstants.getWSResourceImplConfKey(rName), null);
      classes.add(wsResourceClass);
      LOG.info("Added resource " + wsResourceClass);
    }
    for(String fName : featureNames) {
      Class wsFeatureClass = conf.getClass(
          GrillConfConstants.getWSFeatureImplConfKey(fName), null);
      classes.add(wsFeatureClass);
      LOG.info("Added feature " + wsFeatureClass);
    }
    for(String lName : listenerNames) {
      Class wsListenerClass = conf.getClass(
          GrillConfConstants.getWSListenerImplConfKey(lName), null);
      classes.add(wsListenerClass);
      LOG.info("Added listener " + wsListenerClass);
    }
    for(String filterName : filterNames) {
      Class wsFilterClass = conf.getClass(
          GrillConfConstants.getWSFilterImplConfKey(filterName), null);
      classes.add(wsFilterClass);
      LOG.info("Added filter " + wsFilterClass);
    }
    return classes;
  }

}
