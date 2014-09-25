package com.inmobi.grill.server.query.rewrite;

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

import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.query.rewrite.RewriteException;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

public class DriverSpecificRewriteException extends RewriteException {

  @Getter
  Map<GrillDriver, String> driverErrorMessages = new HashMap<GrillDriver, String>();

  public DriverSpecificRewriteException(String msg, Throwable th){
    super(msg, th);
  }

  public DriverSpecificRewriteException() {
    super();
  }

  public DriverSpecificRewriteException(Throwable th) {
    super(th);
  }

  public DriverSpecificRewriteException(String msg) {
    super(msg);
  }

  public void addDriverError(GrillDriver driver, String errorMessage) {
    driverErrorMessages.put(driver, errorMessage);
  }

  @Override
  public String getMessage() {
    StringBuilder errorMessage = new StringBuilder(super.getMessage());
     if(driverErrorMessages != null) {
       for(Map.Entry<GrillDriver, String> driver: driverErrorMessages.entrySet()) {
         errorMessage.append(" Driver :").append(driver.getClass().getName());
         errorMessage.append(" Cause :" ).append(driver.getValue());
       }
     }
     return errorMessage.toString();
  }
}
