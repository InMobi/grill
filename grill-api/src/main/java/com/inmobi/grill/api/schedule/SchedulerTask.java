package com.inmobi.grill.api.schedule;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class SchedulerTask implements Runnable {
  @XmlElement @Getter private String scheduleID;
  @XmlElement @Getter private SchedulerTaskStatus status;
  @XmlElement @Getter private SchedulerType schedulerType;
  // In case of schedulerType = Custom, will take frequencyParams in standard cron format "min hour dayOFMonth Month DayOfWeek"
  @XmlElement @Getter private String frequencyParams;
}
