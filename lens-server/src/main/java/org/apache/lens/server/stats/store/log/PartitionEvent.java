package org.apache.lens.server.stats.store.log;
/*
 * #%L
 * Lens Server
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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

import lombok.Getter;

import java.util.Map;

import org.apache.lens.server.api.events.LensEvent;

/**
 * Event class which encapsulates the partition information.
 */
public class PartitionEvent extends LensEvent {

  private final String eventName;

  private final Map<String, String> partMap;

  @Getter
  private final String className;

  public PartitionEvent(String eventName, Map<String, String> partMap,
                        String className) {
    super(System.currentTimeMillis());
    this.eventName = eventName;
    this.partMap = partMap;
    this.className = className;
  }

  /**
   * Gets the statistics event for which partition event was raised
   *
   * @return name of the event class.
   */
  public String getEventName() {
    return eventName;
  }

  /**
   * Gets the partition map with location of log files to partition key.
   *
   * @return partition map with partition name to log file location.
   */
  public Map<String, String> getPartMap() {
    return partMap;
  }

  @Override
  public String getEventId() {
    return "partition event";
  }
}
