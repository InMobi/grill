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
/*
 *
 */
package org.apache.lens.api.query;

/**
 * The Enum for query submit operations.
 */
public enum SubmitOp {

  /**
<<<<<<< HEAD
   * The execute.
=======
   * Estimate query cost
   */
  ESTIMATE,

  /**
   * Execute the query
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
   */
  EXECUTE,

  /**
<<<<<<< HEAD
   * The explain.
=======
   * Explain the query.
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
   */
  EXPLAIN,

  /**
<<<<<<< HEAD
   * The prepare.
=======
   * Prepare the query.
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
   */
  PREPARE,

  /**
<<<<<<< HEAD
   * The explain and prepare.
=======
   * Explain and prepare the query.
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
   */
  EXPLAIN_AND_PREPARE,

  /**
<<<<<<< HEAD
   * The execute with timeout.
=======
   * Execute query and return result within timeout, if query is successful.
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
   */
  EXECUTE_WITH_TIMEOUT
}
