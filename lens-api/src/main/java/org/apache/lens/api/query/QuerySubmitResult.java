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

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

/**
 * The Class QuerySubmitResult.
 */
@XmlRootElement
@XmlSeeAlso({QueryHandle.class, QueryPrepareHandle.class, QueryHandleWithResultSet.class,
<<<<<<< HEAD
  org.apache.lens.api.query.QueryPlan.class})
=======
  org.apache.lens.api.query.QueryPlan.class, EstimateResult.class})
>>>>>>> e3ff7daa540cc4b0225ee5aa5384bc7cd49c06d7
public abstract class QuerySubmitResult {

}
