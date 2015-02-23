/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.api.session;

<<<<<<< HEAD
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;

import java.util.List;
import java.util.Map;

public interface SessionService {

  /** The Constant NAME. */
  public static final String NAME = "session";
=======
import java.util.List;
import java.util.Map;

import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;

public interface SessionService {

  /** Name of session service */
  String NAME = "session";
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

  /**
   * Open session.
   *
   * @param username      the username
   * @param password      the password
<<<<<<< HEAD
=======
   * @param database      Set current database to the supplied value
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
   * @param configuration the configuration
   * @return the lens session handle
   * @throws LensException the lens exception
   */

<<<<<<< HEAD
  public LensSessionHandle openSession(String username, String password, Map<String, String> configuration)
      throws LensException;
=======
  LensSessionHandle openSession(String username, String password, String database,
                                Map<String, String> configuration)
    throws LensException;
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

  /**
   * Restore session from previous instance of lens server.
   *
   * @param sessionHandle the session handle
   * @param userName      the user name
   * @param password      the password
   * @throws LensException the lens exception
   */

<<<<<<< HEAD
  public void restoreSession(LensSessionHandle sessionHandle, String userName, String password) throws LensException;
=======
  void restoreSession(LensSessionHandle sessionHandle, String userName, String password) throws LensException;
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

  /**
   * Close session.
   *
   * @param sessionHandle the session handle
   * @throws LensException the lens exception
   */

<<<<<<< HEAD
  public void closeSession(LensSessionHandle sessionHandle) throws LensException;
=======
  void closeSession(LensSessionHandle sessionHandle) throws LensException;
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

  /**
   * Adds the resource.
   *
   * @param sessionHandle the session handle
   * @param type          the type
   * @param path          the path
   * @throws LensException the lens exception
   */

<<<<<<< HEAD
  public void addResource(LensSessionHandle sessionHandle, String type, String path);
=======
  void addResource(LensSessionHandle sessionHandle, String type, String path);
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

  /**
   * Delete resource.
   *
   * @param sessionHandle the session handle
   * @param type          the type
   * @param path          the path
   * @throws LensException the lens exception
   */

<<<<<<< HEAD
  public void deleteResource(LensSessionHandle sessionHandle, String type, String path);
=======
  void deleteResource(LensSessionHandle sessionHandle, String type, String path);
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7


  /**
   * Gets the all session parameters.
   *
   * @param sessionHandle the sessionid
<<<<<<< HEAD
   * @param verbose   the verbose
   * @param key       the key
   * @return the all session parameters
   * @throws LensException the lens exception
   */
  public List<String> getAllSessionParameters(LensSessionHandle sessionHandle, boolean verbose, String key)
      throws LensException;
=======
   * @param verbose       the verbose
   * @param key           the key
   * @return the all session parameters
   * @throws LensException the lens exception
   */
  List<String> getAllSessionParameters(LensSessionHandle sessionHandle, boolean verbose, String key)
    throws LensException;
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

  /**
   * Sets the session parameter.
   *
   * @param sessionHandle the sessionid
<<<<<<< HEAD
   * @param key       the key
   * @param value     the value
   */
  public void setSessionParameter(LensSessionHandle sessionHandle, String key, String value);
=======
   * @param key           the key
   * @param value         the value
   */
  void setSessionParameter(LensSessionHandle sessionHandle, String key, String value);
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

  /**
   * Adds the resource to all services.
   *
   * @param sessionHandle the sessionid
<<<<<<< HEAD
   * @param type the type
   * @param path the path
   * @return the number of services that the resource has been added to
   */
  public int addResourceToAllServices(LensSessionHandle sessionHandle, String type, String path);
=======
   * @param type          the type
   * @param path          the path
   * @return the number of services that the resource has been added to
   */

  int addResourceToAllServices(LensSessionHandle sessionHandle, String type, String path);
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7

  /**
   * Lists resources from the session service
   *
   * @param sessionHandle the sessionid
   * @param type          the resource type, can be null, file or jar
   * @return   Lists resources for a given resource type.
   *           Lists all resources if resource type is null
   */
  List<String> listAllResources(LensSessionHandle sessionHandle, String type);
}
