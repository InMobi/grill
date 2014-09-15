package com.inmobi.grill.client;

/*
 * #%L
 * Grill client
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


import com.inmobi.grill.api.GrillConf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Top level class which encapsulates connections parameters required for grill
 * connection.
 */
public class GrillConnectionParams {

  private Map<String, String> grillConfs = new HashMap<String, String>();
  private Map<String, String> grillVars = new HashMap<String, String>();
  private Map<String, String> sessionVars = new HashMap<String, String>();

  private final GrillClientConfig conf;

  /**
   * Construct parameters required to connect to grill server using values in
   * grill-client-site.xml
   */
  public GrillConnectionParams() {
    this.conf = new GrillClientConfig();
  }

  /**
   * Construct parameters required to connect to grill server from values passed
   * in configuration.
   *
   * @param conf from which connection parameters are defined.
   */
  public GrillConnectionParams(GrillClientConfig conf) {
    this.conf = conf;
  }

  /**
   * Gets host to which grill client should connect to.
   *
   * @return hostname of grill server
   */
  public String getHost() {
    return conf.getGrillHost();
  }

  /**
   * Gets the port to which grill client should connect to.
   *
   * @return portnumber of grill server
   */
  public int getPort() {
    return conf.getGrillPort();
  }

  /**
   * Gets the Database to which grill client should connect to.
   *
   * @return database to connect to
   */
  public String getDbName() {
    return conf.getGrillDatabase();
  }


  public Map<String, String> getGrillConfs() {
    return grillConfs;
  }

  public Map<String, String> getGrillVars() {
    return grillVars;
  }

  public Map<String, String> getSessionVars() {
    return sessionVars;
  }

  public void setHost(String host) {
    this.conf.setGrillHost(host);
  }

  public void setPort(int port) {
    this.conf.setGrillPort(port);
  }

  public void setDbName(String dbName) {
    this.conf.setGrillDatabase(dbName);
  }


  public String getBaseConnectionUrl() {
    return "http://" + this.getHost() + ":"
        + this.getPort() + "/"
        + this.conf.getAppBasePath();
  }

  public GrillClientConfig getConf() {
    return this.conf;
  }

  public String getUser() {
    return this.conf.getUser();
    /*
    return this.sessionVars.get("user.name")!= null ?
        this.sessionVars.get("user.name") : "";*/
  }

  public String getPassword() {
    return this.conf.getPassword();
  }

  public String getSessionResourcePath() {
    return this.conf.getSessionResourcePath();
  }

  public String getQueryResourcePath() {
    return this.conf.getQueryResourcePath();
  }

  public String getMetastoreResourcePath() {
    return this.conf.getMetastoreResourcePath();
  }

  public long getQueryPollInterval() {
    return this.conf.getQueryPollInterval();
  }


  public GrillConf getSessionConf() {
    GrillConf conf = new GrillConf();
    Iterator<Map.Entry<String, String>> itr = this.conf.iterator();
    while (itr.hasNext()) {
      Map.Entry<String, String> entry = itr.next();
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : grillConfs.entrySet()) {
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : sessionVars.entrySet()) {
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : grillVars.entrySet()) {
      conf.addProperty(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  @Override
  public String toString() {
    return new StringBuilder("GrillConnectionParams{")
    .append("dbName='").append(this.conf.getGrillDatabase()).append('\'')
    .append(", host='").append(this.conf.getGrillHost()).append('\'')
    .append(", port=").append(this.conf.getGrillPort())
    .append(", user=").append(this.conf.getUser())
    .append(", grillConfs=").append(grillConfs)
    .append(", grillVars=").append(grillVars)
    .append(", sessionVars=").append(sessionVars)
    .append('}')
    .toString();
  }
}
