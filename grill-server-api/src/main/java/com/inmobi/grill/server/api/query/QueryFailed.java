package com.inmobi.grill.server.api.query;

/*
 * #%L
 * Grill API for server and extensions
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

import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;

/**
 * Event fired when a query fails to execute. Use getCause() to get the reason for failure.
 */
public class QueryFailed extends QueryEnded {
  public QueryFailed(long eventTime, QueryStatus.Status prev,
      QueryStatus.Status current, QueryHandle handle, String user, String cause) {
    super(eventTime, prev, current, handle, user, cause);
    checkCurrentState(QueryStatus.Status.FAILED);
  }
}
