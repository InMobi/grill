package com.inmobi.grill.server.api.scheduler;

import org.quartz.Job;

import com.inmobi.grill.api.GrillException;

public interface Execution extends Job {
  public String getId() throws GrillException;

  public String getOutputURI() throws GrillException;

  public String getStatus() throws GrillException;
}
