package com.inmobi.grill.server.stats.store.log;
 /*
 * #%L
 * Grill Server
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

import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.events.GrillEventService;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.Timer;

/**
 * Class which handles the log4j rolled file.
 */
public class StatisticsLogRollupHandler {



  private com.inmobi.grill.server.stats.store.log.StatisticsLogFileScannerTask task;
  private Timer timer;
  private long rate;
  private final ConcurrentHashSet<String> scanSet = new ConcurrentHashSet<String>();
  /**
   * Initalize the handler.
   *
   * @param conf configuration to be used while initialization.
   */
  public void initialize(Configuration conf) {
    task = new StatisticsLogFileScannerTask();
    timer = new Timer();
    rate = conf.getLong(GrillConfConstants.GRILL_STATS_ROLLUP_SCAN_RATE,
        GrillConfConstants.DEFAULT_STATS_ROLLUP_SCAN_RATE);
  }

  public void start(GrillEventService service) {
    task.setService(service);
    timer.scheduleAtFixedRate(task, rate, rate);
  }


  public void stop() {
    timer.cancel();
  }


  public void addToScanTask(String event) {
    if (!scanSet.contains(event)) {
      scanSet.add(event);
      task.addLogFile(event);
    }
  }

}

