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
public class ScheduleRunInfo {
  // Gives insights of an instance of a schedule to user
  @XmlElement @Getter String scheduleHandle;
  @XmlElement @Getter String scheduleRunHandle;
  @XmlElement @Getter Object task;
  @XmlElement @Getter String startTime;
  @XmlElement @Getter String endTime;
  @XmlElement @Getter String resultSetPath;
  @XmlElement @Getter ScheduleTaskStatus taskStatus;
}
