package com.inmobi.grill.server.ml.spark;

import com.inmobi.grill.server.GrillApplicationListener;
import com.inmobi.grill.server.ml.MLServiceResource;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

@ApplicationPath("/session")
public class MLApp extends Application {
  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> classes = new HashSet<Class<?>>();
    // register root resource
    classes.add(MLServiceResource.class);
    classes.add(MultiPartFeature.class);
    classes.add(LoggingFilter.class);
    classes.add(GrillApplicationListener.class);
    return classes;
  }
}
