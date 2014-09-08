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
public class GrillSchedule {
  @XmlElement @Getter private GrillScheduleHandle scheduleHandle;
  @XmlElement @Getter private JobType type;
  @XmlElement @Getter private Object task;
  // Task object which will represent QueryTask/etc., will be casted based on JobType
  @XmlElement @Getter private ScheduleType scheduleType;
  // In case of scheduleType = Custom, will take frequencyParams in standard cron format "min hour dayOFMonth Month DayOfWeek"
  @XmlElement @Getter private String frequencyParams;
  // startTime : from when Schedule should start running
  @XmlElement @Getter private String startTime;
  // endTime : till when Schedule should run
  @XmlElement @Getter private String endTime;
}
