package org.apache.lens.server.ui;

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

import org.apache.lens.server.AuthenticationFilter;
import org.apache.lens.server.GrillApplicationListener;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

@ApplicationPath("/ui")
public class UIApp extends Application {

  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();
    classes.add(StaticFileResource.class);
    classes.add(QueryServiceUIResource.class);
    classes.add(SessionUIResource.class);
    classes.add(MetastoreUIResource.class);
    classes.add(MultiPartFeature.class);
    classes.add(AuthenticationFilter.class);
    classes.add(GrillApplicationListener.class);
    return classes;
  }

}
