package com.inmobi.grill.api.schedule;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PUBLIC)
public class ScheduleRunInfo {
  // Gives insights of an instance of a schedule to user
  @XmlElement
  @Getter
  @Setter
  String scheduleid;
  @XmlElement
  @Getter
  @Setter
  String runHandle;
  @XmlElement
  @Getter
  @Setter
  String startTime;
  @XmlElement
  @Getter
  @Setter
  String endTime;
  @XmlElement
  @Getter
  @Setter
  String resultSetPath;
  @XmlElement
  @Getter
  @Setter
  String taskStatus;
}
