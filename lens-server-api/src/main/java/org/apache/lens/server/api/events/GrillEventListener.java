package org.apache.lens.server.api.events;

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


import org.apache.lens.api.GrillException;

/**
 * <p>
 *   The handler method should not block so that the event service can proceed to notifying other listeners
 *   as soon as possible. Any resource intensive computation related to the event must be done offline.
 * </p>
 * @param <T>
 */
public interface GrillEventListener<T extends GrillEvent> {
  // If the event handler method is renamed, the following constant must be changed as well
  public static final String HANDLER_METHOD_NAME = "onEvent";
  public void onEvent(T event) throws GrillException;
}