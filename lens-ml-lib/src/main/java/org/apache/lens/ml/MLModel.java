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

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * Instantiates a new ML model.
 */
@NoArgsConstructor
public abstract class MLModel<PREDICTION> implements Serializable {

  /** The id. */
  @Getter
  @Setter
  private String id;

  /** The created at. */
  @Getter
  @Setter
  private Date createdAt;

  /** The trainer name. */
  @Getter
  @Setter
  private String trainerName;

  /** The table. */
  @Getter
  @Setter
  private String table;

  /** The params. */
  @Getter
  @Setter
  private List<String> params;

  /** The label column. */
  @Getter
  @Setter
  private String labelColumn;

  /** The feature columns. */
  @Getter
  @Setter
  private List<String> featureColumns;

  /**
   * Predict.
   *
   * @param args
   *          the args
   * @return the prediction
   */
  public abstract PREDICTION predict(Object... args);
}
