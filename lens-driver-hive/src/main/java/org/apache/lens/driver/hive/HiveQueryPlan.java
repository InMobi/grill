package org.apache.lens.driver.hive;

/*
 * #%L
 * Lens Hive Driver
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.lens.api.query.QueryCost;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.DriverQueryPlan;


public class HiveQueryPlan extends DriverQueryPlan {
  private String explainOutput;
  private Map<String, List<String>> partitions;

  enum ParserState {
    BEGIN,
    FILE_OUTPUT_OPERATOR,
    TABLE_SCAN,
    JOIN,
    SELECT,
    GROUPBY,
    GROUPBY_KEYS,
    GROUPBY_EXPRS,
    MOVE,
    MAP_REDUCE,
    PARTITION_LIST,
    PARTITION,
  };

  public HiveQueryPlan(List<String> explainOutput, QueryPrepareHandle prepared,
      HiveConf metastoreConf) throws HiveException {
    setPrepareHandle(prepared);
    setExecMode(ExecMode.BATCH);
    setScanMode(ScanMode.PARTIAL_SCAN);
    partitions = new LinkedHashMap<String, List<String>>();
    extractPlanDetails(explainOutput, metastoreConf);
    this.explainOutput = StringUtils.join(explainOutput, '\n');
  }

  private void extractPlanDetails(List<String> explainOutput, HiveConf metastoreConf) throws HiveException {
    ParserState state = ParserState.BEGIN;
    ParserState prevState = state;
    ArrayList<ParserState> states = new ArrayList<ParserState>();
    Hive metastore = Hive.get(metastoreConf);
    List<String> partList = null;

    for (int i = 0; i < explainOutput.size(); i++) {
      String line = explainOutput.get(i);
      String tr = line.trim();
      prevState = state;
      state = nextState(tr, state);

      if (prevState != state) {
        states.add(prevState);
      }

      switch (state) {
        case MOVE:
          if (tr.startsWith("destination:")) {
            String outputPath = tr.replace("destination:", "").trim();
            resultDestination = outputPath;
          }
          break;
        case TABLE_SCAN:
          if (tr.startsWith("alias:")) {
            String tableName = tr.replace("alias:", "").trim();
            tablesQueried.add(tableName);
            Table tbl = metastore.getTable(tableName);
            String costStr = tbl.getParameters().get(LensConfConstants.STORAGE_COST);
            
            Double weight = 1d;
            if (costStr != null) {
              weight = Double.parseDouble(costStr);
            }
            tableWeights.put(tableName, weight);
          }
          break;
        case JOIN:
          if (tr.equals("condition map:")) {
            numJoins++;
          }
          break;
        case SELECT:
          if (tr.startsWith("expressions:") && states.get(states.size() - 1) == ParserState.TABLE_SCAN) {
            numSels += StringUtils.split(tr, ",").length;
          }
          break;
        case GROUPBY_EXPRS:
          if (tr.startsWith("aggregations:")) {
            numAggrExprs += StringUtils.split(tr, ",").length;
          }
          break;
        case GROUPBY_KEYS:
          if (tr.startsWith("keys:")) {
            numGbys += StringUtils.split(tr, ",").length;
          }
          break;
        case PARTITION:
          if (tr.equals("partition values:")) {
            i++;
            List<String> partVals = new ArrayList<String>();
            // Look ahead until we reach partition properties
            String lineAhead = null;
            for (; i < explainOutput.size(); i++ ) {
              if (explainOutput.get(i).trim().equals("properties:")) {
                break;
              }
              lineAhead = explainOutput.get(i).trim();
              partVals.add(lineAhead);
            }

            String partConditionStr = StringUtils.join(partVals, ";");

            // Now seek table name
            for (; i < explainOutput.size(); i++) {
              if (explainOutput.get(i).trim().startsWith("name:")) {
                String table = explainOutput.get(i).trim().substring("name:".length()).trim();
                List<String> tablePartitions = partitions.get(table);
                if (tablePartitions == null) {
                  tablePartitions = new ArrayList<String>();
                  partitions.put(table, tablePartitions);
                }
                tablePartitions.add(partConditionStr);

                break;
              }
            }
          }

          break;
      }
    }
	}

  private ParserState nextState(String tr, ParserState state) {
    if (tr.equals("File Output Operator")) {
      return ParserState.FILE_OUTPUT_OPERATOR;
    } else if (tr.equals("Map Reduce")) {
      return ParserState.MAP_REDUCE;
    } else if (tr.equals("Move Operator")) {
      return ParserState.MOVE;
    } else if (tr.equals("TableScan")) {
      return ParserState.TABLE_SCAN;
    } else if (tr.equals("Map Join Operator")) {
      return ParserState.JOIN;
    } else if (tr.equals("Select Operator")) {
      return ParserState.SELECT;
    } else if (tr.equals("Group By Operator")) {
      return ParserState.GROUPBY;
    } else if (tr.startsWith("aggregations:") && state == ParserState.GROUPBY) {
      return ParserState.GROUPBY_EXPRS;
    } else if (tr.startsWith("keys:") && state == ParserState.GROUPBY_EXPRS) {
      return ParserState.GROUPBY_KEYS;
    } else if (tr.equals("Path -> Partition:")) {
      return ParserState.PARTITION_LIST;
    } else if (tr.equals("Partition") && state == ParserState.PARTITION_LIST) {
      return ParserState.PARTITION;
    }
    return state;
  }

  @Override
	public String getPlan() {
		return explainOutput;
	}

	@Override
	public QueryCost getCost() {
    /*
    Return query cost as 1 so that if JDBC storage and other storage is present,
    JDBC is given preference.
     */
		return new QueryCost(1,1);
	}

  @Override
  public Map<String, List<String>> getPartitions() {
    return partitions;
  }
}
