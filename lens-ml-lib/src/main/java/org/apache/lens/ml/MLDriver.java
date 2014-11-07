/*
 * #%L
 * Lens ML Lib
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
package org.apache.lens.ml;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;

import java.util.List;

/**
 * The Interface MLDriver.
 */
public interface MLDriver {

  /**
   * Checks if is trainer supported.
   *
   * @param trainer
   *          the trainer
   * @return true, if is trainer supported
   */
  public boolean isTrainerSupported(String trainer);

  /**
   * Gets the trainer instance.
   *
   * @param trainer
   *          the trainer
   * @return the trainer instance
   * @throws LensException
   *           the lens exception
   */
  public MLTrainer getTrainerInstance(String trainer) throws LensException;

  /**
   * Inits the.
   *
   * @param conf
   *          the conf
   * @throws LensException
   *           the lens exception
   */
  public void init(LensConf conf) throws LensException;

  /**
   * Start.
   *
   * @throws LensException
   *           the lens exception
   */
  public void start() throws LensException;

  /**
   * Stop.
   *
   * @throws LensException
   *           the lens exception
   */
  public void stop() throws LensException;

  public List<String> getTrainerNames();
}
