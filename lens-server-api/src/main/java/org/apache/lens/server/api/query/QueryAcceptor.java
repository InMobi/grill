package org.apache.lens.server.api.query;

/*
 * #%L
 * Lens API for server and extensions
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

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.SubmitOp;


public interface QueryAcceptor {

  /**
   * Whether to accept the query or not
   * 
   * @param query The query
   * @param conf The configuration of the query
   *
   * @return null if query should be accepted, rejection cause otherwise
   * 
   * @throws LensException
   */
  public String accept(String query, Configuration conf, SubmitOp submitOp) throws LensException;

}
