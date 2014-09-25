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
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ScheduleRunInfo {
  // Gives insights of an instance of a schedule to user
  @Getter
  @Setter
  @XmlElement
  private String scheduleid;
  @Getter
  @Setter
  @XmlElement
  private String runHandle;
  @Getter
  @Setter
  @XmlElement
  private long startTime;
  @Getter
  @Setter
  @XmlElement
  private long endTime;
  @Getter
  @Setter
  @XmlElement
  private String resultSetPath;
  @Getter
  @Setter
  @XmlElement
  private String taskStatus;
}
