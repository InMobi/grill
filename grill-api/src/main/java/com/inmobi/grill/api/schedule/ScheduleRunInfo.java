package com.inmobi.grill.api.schedule;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ScheduleRunInfo {
  // Gives insights of an instance of a schedule to user
  @Getter
  @Setter
  private String scheduleid;
  @Getter
  @Setter
  private String runHandle;
  @Getter
  @Setter
  private long startTime;
  @Getter
  @Setter
  private long endTime;
  @Getter
  @Setter
  private String resultSetPath;
  @Getter
  @Setter
  private String taskStatus;
}
