package com.inmobi.grill.server.scheduler;

import org.quartz.Job;

import com.inmobi.grill.api.GrillException;

public interface Execution extends Job {
  public String getId() throws GrillException;
}
