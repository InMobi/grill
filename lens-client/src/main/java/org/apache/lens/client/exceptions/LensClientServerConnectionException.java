package org.apache.lens.client.exceptions;

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

public class LensClientServerConnectionException extends LensClientException {
  private static final long serialVersionUID = 1L;
  private int errorCode;

  public LensClientServerConnectionException(int errorCode) {
    super("Server Connection gave error code " + errorCode);
    this.errorCode = errorCode;
  }

  public LensClientServerConnectionException(String message, Exception e) {
    super(message, e);
  }

  public int getErrorCode() {
    return errorCode;
  }
}
