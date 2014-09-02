package com.inmobi.grill.api.schedule;

import javax.xml.bind.annotation.XmlElement;

import lombok.Getter;

public class SchedulerQuery extends SchedulerTask {
  @XmlElement @Getter private String query;

  @Override
  public void run() {
    // TODO Auto-generated method stub
  }

}
