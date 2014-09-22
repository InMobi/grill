package com.inmobi.grill.server.api.scheduler;

/*
 * #%L
 * Grill Hive Driver
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

import java.sql.Clob;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Class to represent the Schedule Info table which is serialized to
 * database.
 */
@EqualsAndHashCode
@ToString
public class ScheduleInfoDAO {

  @Getter
  @Setter
  String schedule_id;
  @Getter
  @Setter
  Clob execution;
  @Getter
  @Setter
  Clob start_spec;
  @Getter
  @Setter
  Clob resource_path;
  @Getter
  @Setter
  Clob schedule_conf;
  @Getter
  @Setter
  int start_time;
  @Getter
  @Setter
  int end_time;
  @Getter
  @Setter
  String username;
  @Getter
  @Setter
  String status;
  @Getter
  @Setter
  Long created_on;

  public ScheduleInfoDAO() {
  }
}
