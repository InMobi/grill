package org.apache.lens.driver.impala;

/*
 * #%L
 * Grill Driver for Cloudera Impala
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;


import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.beeswax.api.BeeswaxException;
import com.cloudera.beeswax.api.Query;
import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.QueryNotFoundException;
import com.cloudera.beeswax.api.QueryState;
import com.cloudera.impala.thrift.ImpalaService;
import com.cloudera.impala.thrift.ImpalaService.Client;

public class ImpalaDriver implements LensDriver {

  Logger logger = Logger.getLogger(ImpalaDriver.class.getName());

  private Client client;
  private List<String> storages = new ArrayList<String>();

  public ImpalaDriver() {
  }

  @Override
  public DriverQueryPlan explain(String query, Configuration conf) {
    /*QueryCost q = new QueryCost();
    q.setExecMode(ExecMode.INTERACTIVE);
    q.setScanMode(ScanMode.FULL_SCAN);
    q.setScanSize(-1);

    return q;*/
    return null;
  }

  public LensResultSet execute(String query, Configuration conf)
      throws LensException {
    Query q = new Query();
    q.query = query;
    QueryHandle queryHandle;
    try {
      queryHandle = client.query(q);
      QueryState qs = QueryState.INITIALIZED;

      while (true) {
        qs = client.get_state(queryHandle);
        logger.info("Query state is for query" + q + "is " + qs);
        if (qs == QueryState.FINISHED) {
          break;
        }
        if (qs == QueryState.EXCEPTION) {
          logger.error("Query aborted, unable to fetch data");
          throw new LensException(
              "Query aborted, unable to fetch data");
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          logger.error(e.getMessage(), e);
          throw new LensException(e.getMessage(), e);
        }
      }

    } catch (BeeswaxException e) {
      logger.error(e.getMessage(), e);
      throw new LensException(e.getMessage(), e);

    } catch (TException e) {
      logger.error(e.getMessage(), e);
      throw new LensException(e.getMessage(), e);

    } catch (QueryNotFoundException e) {
      logger.error(e.getMessage(), e);
      throw new LensException(e.getMessage(), e);

    }

    ImpalaResultSet iResultSet = new ImpalaResultSet(client, queryHandle);
    return iResultSet;

  }

  @Override
  public void configure(Configuration conf) throws LensException {
    final String HOST = "HOST";
    final String PORT = "PORT";
    TSocket sock = new TSocket(conf.get(HOST), conf.getInt(PORT, 9999));
    try {
      sock.open();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
      throw new LensException(e.getMessage(), e);
    }
    TBinaryProtocol protocol = new TBinaryProtocol(sock);
    this.client = new ImpalaService.Client(protocol);
    logger.info("Successfully connected to host" + conf.get(HOST) + ":"
        + conf.getInt(PORT, 9999));

  }

  @Override
  public void updateStatus(QueryContext context) {
    // TODO Auto-generated method stub
    return;
  }

  @Override
  public boolean cancelQuery(org.apache.lens.api.query.QueryHandle handle) {
    // TODO Auto-generated method stub
    return false;
  }

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws LensException {
		// TODO Auto-generated method stub
		
	}

  /**
   * Add a listener for driver events
   *
   * @param driverEventListener
   */
  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {

  }

  @Override
  public void closeQuery(org.apache.lens.api.query.QueryHandle arg0)
      throws LensException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws LensException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle)
      throws LensException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public LensResultSet execute(QueryContext context) throws LensException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void executeAsync(QueryContext context) throws LensException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public LensResultSet fetchResultSet(QueryContext context)
      throws LensException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void closeResultSet(org.apache.lens.api.query.QueryHandle handle)
      throws LensException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void registerForCompletionNotification(
      org.apache.lens.api.query.QueryHandle handle, long timeoutMillis,
      QueryCompletionListener listener) throws LensException {
    throw new LensException("Not implemented");    
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // TODO Auto-generated method stub
    
  }

}
