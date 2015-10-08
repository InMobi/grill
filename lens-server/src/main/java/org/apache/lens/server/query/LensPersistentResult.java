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
package org.apache.lens.server.query;

import java.io.IOException;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.PersistentResultSet;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.FinishedLensQuery;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** The Class LensPersistentResult. */
@Slf4j
public class LensPersistentResult extends PersistentResultSet {

  /** The metadata. */
  private final LensResultSetMetadata metadata;

  /** The output path. */
  private final String outputPath;

  /** The num rows. */
  private final Integer numRows;

  /** The file size. */
  private final Long fileSize;
  private final Configuration conf;
  @Getter
  private String httpResultUrl = null;

  /**
   * Instantiates a new lens persistent result.
   *  @param queryHandle the query handle
   * @param metadata    the metadata
   * @param outputPath  the output path
   * @param numRows     the num rows
   * @param conf        the lens server conf
   */
  public LensPersistentResult(QueryHandle queryHandle, LensResultSetMetadata metadata, String outputPath, Integer
    numRows, Long fileSize,
    Configuration conf) {
    this.metadata = metadata;
    this.outputPath = outputPath;
    this.numRows = numRows;
    this.fileSize = fileSize;
    this.conf = conf;
    if (isHttpResultAvailable()) {
      this.httpResultUrl = conf.get(LensConfConstants.SERVER_BASE_URL, LensConfConstants.DEFAULT_SERVER_BASE_URL)
        + "queryapi/queries/" + queryHandle + "/httpresultset";
    }
  }

  public LensPersistentResult(QueryContext ctx, Configuration conf) {
    this(ctx.getQueryHandle(),
      ctx.getQueryOutputFormatter().getMetadata(),
      ctx.getQueryOutputFormatter().getFinalOutputPath(),
      ctx.getQueryOutputFormatter().getNumRows(),
      ctx.getQueryOutputFormatter().getFileSize(), conf);
  }

  public LensPersistentResult(FinishedLensQuery query, Configuration conf) throws
    ClassNotFoundException, IOException {
    this(QueryHandle.fromString(query.getHandle()),
      LensResultSetMetadata.fromJson(query.getMetadata()),
      query.getResult(), query.getRows(), query.getFileSize(), conf);
  }

  @Override
  public String getOutputPath() throws LensException {
    return outputPath;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensResultSet#size()
   */
  @Override
  public Integer size() throws LensException {
    return numRows;
  }

  @Override
  public Long getFileSize() throws LensException {
    return fileSize;
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    return metadata;
  }

  @Override
  public boolean isHttpResultAvailable() {
    try {
      final Path resultPath = new Path(getOutputPath());
      FileSystem fs = resultPath.getFileSystem(conf);
      if (fs.isDirectory(resultPath)) {
        return false;
      }
    } catch (IOException | LensException e) {
      log.warn("Unable to get status for Result Directory", e);
      return false;
    }
    return true;
  }
}
