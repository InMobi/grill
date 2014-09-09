package com.inmobi.grill.api.schedule;

/*
 * #%L
 * Grill API
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import com.inmobi.grill.api.Priority;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ScheduleInfo {
  // Schedule Info gives insights to user about a schedule ID
  @XmlElement @Getter private GrillScheduleHandle scheduleHandle;
  // XSD object task
  @XmlElement @Getter private XSchedule schedule; // Needs to be changed to schedule
  @XmlElement @Getter private String submittedUser;
  // startTime : from when Schedule should start running
  @XmlElement @Getter private String startTime;
  // endTime : till when Schedule should run
  @XmlElement @Getter private String endTime;
  @XmlElement @Getter private Priority priority;
  @XmlElement @Getter private String lastRunInstance;
  @XmlElement @Getter private int runcount;
  @XmlElement @Getter private String latestResultSetPath;
  @XmlElement @Getter private ScheduleStatus scheduleStatus; 
}
