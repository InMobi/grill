/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.api.scheduler;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POJO for an instance of SchedulerJob.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement
public class SchedulerJobInstanceInfo {

  /**
   * @param id new id for the instance of scheduler job.
   * @return unique id for this instance of scheduler job.
   */
  private SchedulerJobInstanceHandle id;

  /**
   * @param jobId new id for the scheduler job.
   * @return id for the scheduler job to which this instance belongs.
   */
  private SchedulerJobHandle jobId;

  /**
   * @param scheduleTime time to be set as the nomial time for the instance.
   * @return scheduled time of this instance.
   */
  private long scheduleTime;

  /**
   * @param instanceRunList: List of runs.
   * @return A list of instance-run for this instance.
   */
  private List<SchedulerJobInstanceRun> instanceRunList;

}
