package com.inmobi.grill.server.query;

/*
 * #%L
 * Grill Server
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

import java.util.List;

import javax.lang.model.element.Name;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.inmobi.grill.api.query.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataParam;

import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.APIResult.Status;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.query.QueryExecutionService;

/**
 * queryapi resource 
 * 
 * This provides api for all things query. 
 *
 */
@Path("/queryapi")
public class QueryServiceResource {
  public static final Logger LOG = LogManager.getLogger(QueryServiceResource.class);

  private QueryExecutionService queryServer;

  private void checkSessionId(GrillSessionHandle sessionHandle) {
    if (sessionHandle == null) {
      throw new BadRequestException("Invalid session handle");
    }
  }

  private void checkQuery(String query) {
    if (StringUtils.isBlank(query)) {
      throw new BadRequestException("Invalid query");
    }
  }

  private SubmitOp getSubmitOp(String operation) {
    SubmitOp sop = null;
    try {
      sop = SubmitOp.valueOf(operation.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }
    if (sop == null) {
      throw new BadRequestException("Invalid operation type: " + operation +
        submitClue);
    }
    return sop;
  }

  /**
   * API to know if Query service is up and running
   * 
   * @return Simple text saying it up
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Queryapi is up";
  }

  public QueryServiceResource() throws GrillException {
    queryServer = (QueryExecutionService)GrillServices.get().getService("query");
  }

  QueryExecutionService getQueryServer() {
    return queryServer;
  }

  /**
   * Get all the queries in the query server; can be filtered with state and user.
   * 
   * @param sessionid The sessionid in which user is working
   * @param state If any state is passed, all the queries in that state will be returned,
   * otherwise all queries will be returned. Possible states are {@value QueryStatus.Status#values()}
   * @param user If any user is passed, all the queries submitted by the user will be returned,
   * otherwise all the queries will be returned
   * 
   * @return List of {@link QueryHandle} objects
   */
  @GET
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public List<QueryHandle> getAllQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    checkSessionId(sessionid);
    try {
      return queryServer.getAllQueries(sessionid, state, user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  static String submitClue = ". supported values are:" + SubmitOp.EXPLAIN  
      + ", " + SubmitOp.EXECUTE + " and " + SubmitOp.EXECUTE_WITH_TIMEOUT;
  static String prepareClue = ". supported values are:" + SubmitOp.PREPARE
      + " and " + SubmitOp.EXPLAIN_AND_PREPARE;
  static String submitPreparedClue = ". supported values are:" 
      + SubmitOp.EXECUTE + " and " + SubmitOp.EXECUTE_WITH_TIMEOUT;

  /**
   * Submit the query for explain or execute or execute with a timeout
   * 
   * @param sessionid The session in which user is submitting the query. Any
   *  configuration set in the session will be picked up.
   * @param query The query to run
   * @param operation The operation on the query. Supported operations are
   * {@value SubmitOp#EXPLAIN}, {@value SubmitOp#EXECUTE} and {@value SubmitOp#EXECUTE_WITH_TIMEOUT}
   * @param conf The configuration for the query
   * @param timeoutmillis The timeout for the query, honored only in case of
   *  {@value SubmitOp#EXECUTE_WITH_TIMEOUT} operation
   * 
   * @return {@link QueryHandle} in case of {@value SubmitOp#EXECUTE} operation.
   * {@link QueryPlan} in case of {@value SubmitOp#EXPLAIN} operation.
   * {@link QueryHandleWithResultSet} in case {@value SubmitOp#EXECUTE_WITH_TIMEOUT} operation.
   */
  @POST
  @Path("queries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult query(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("query") String query,
      @FormDataParam("operation") String operation,
      @FormDataParam("conf") GrillConf conf,
      @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutmillis) {
    checkQuery(query);
    checkSessionId(sessionid);
    try {
      switch (getSubmitOp(operation)) {
      case EXECUTE:
        return queryServer.executeAsync(sessionid, query, conf);
      case EXPLAIN:
        return queryServer.explain(sessionid, query, conf);
      case EXECUTE_WITH_TIMEOUT:
        return queryServer.execute(sessionid, query, timeoutmillis, conf);
      default:
        throw new BadRequestException("Invalid operation type: " + operation + submitClue);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Cancel all the queries in query server; can be filtered with state and user
   * 
   * @param sessionid The session in which cancel is issued
   * @param state If any state is passed, all the queries in that state will be cancelled,
   * otherwise all queries will be cancelled. Possible states are {@value QueryStatus.Status#values()}
   * The queries in {@value QueryStatus.Status#FAILED},{@value QueryStatus.Status#FAILED},
    {@value QueryStatus.Status#CLOSED}, {@value QueryStatus.Status#UNKNOWN} cannot be cancelled
   * @param user If any user is passed, all the queries submitted by the user will be cancelled,
   * otherwise all the queries will be cancelled
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful cancellation.
   * APIResult with state {@value Status#FAILED} in case of cancellation failure.
   * APIResult with state {@value Status#PARTIAL} in case of partial cancellation.
   */
  @DELETE
  @Path("queries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelAllQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("state") String state,
      @DefaultValue("") @QueryParam("user") String user) {
    checkSessionId(sessionid);
    int numCancelled = 0;
    List<QueryHandle> handles = null;
    boolean failed = false;
    try {
      handles = getAllQueries(sessionid, state, user);
      for (QueryHandle handle : handles) {
        if (cancelQuery(sessionid, handle)) {
          numCancelled++;
        }
      }
    } catch (Exception e) {
      LOG.error("Error canceling queries", e);
      failed = true;
    }
    String msgString = (StringUtils.isBlank(state) ? "" : " in state" + state)
        + (StringUtils.isBlank(user) ? "" : " for user " + user);
    if (handles != null && numCancelled == handles.size()) {
      return new APIResult(Status.SUCCEEDED, "Cancel all queries "
          + msgString + " is successful");
    } else {
      assert (failed);
      if (numCancelled == 0) {
        return new APIResult(Status.FAILED, "Cancel on the query "
            + msgString + " has failed");        
      } else {
        return new APIResult(Status.PARTIAL, "Cancel on the query "
            + msgString + " is partial");        
      }
    }
  }

  /**
   * Get all prepared queries in the query server; can be filtered with user
   * 
   * @param sessionid The sessionid in which user is working
   * @param user If any user is passed, all the queries prepared by the user will be returned,
   * otherwise all the queries will be returned
   * 
   * @return List of QueryPrepareHandle objects
   */
  @GET
  @Path("preparedqueries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public List<QueryPrepareHandle> getAllPreparedQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("user") String user) {
    checkSessionId(sessionid);
    try {
      return queryServer.getAllPreparedQueries(sessionid, user);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Prepare a query or 'explain and prepare' the query
   * 
   * @param sessionid The session in which user is preparing the query. Any
   *  configuration set in the session will be picked up.
   * @param query The query to prepare
   * @param operation The operation on the query. Supported operations are
   * {@value SubmitOp#EXPLAIN_AND_PREPARE} or {@value SubmitOp#PREPARE}
   * @param conf The configuration for preparing the query
   * 
   * @return {@link QueryPrepareHandle} incase of {@value SubmitOp#PREPARE} operation.
   * {@link QueryPlan} incase of {@value SubmitOp#EXPLAIN_AND_PREPARE} and the 
   * query plan will contain the prepare handle as well.
   */
  @POST
  @Path("preparedqueries")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult prepareQuery(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @FormDataParam("query") String query,
      @DefaultValue("") @FormDataParam("operation") String operation,
      @FormDataParam("conf") GrillConf conf) {
    try {
      checkSessionId(sessionid);
      checkQuery(query);
      SubmitOp sop = null;
      try {
        sop = SubmitOp.valueOf(operation.toUpperCase());
      } catch (IllegalArgumentException e) {
      }
      if (sop == null) {
        throw new BadRequestException("Invalid operation type: " + operation + prepareClue);
      }
      switch (sop) {
      case PREPARE:
        return queryServer.prepare(sessionid, query, conf);
      case EXPLAIN_AND_PREPARE:
        return queryServer.explainAndPrepare(sessionid, query, conf);
      default:
        throw new BadRequestException("Invalid operation type: " + operation + prepareClue);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Destroy all the prepared queries; Can be filtered with user
   * 
   * @param sessionid The session in which cancel is issued
   * @param user If any user is passed, all the queries prepared by the user will be destroyed,
   * otherwise all the queries will be destroyed
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful destroy.
   * APIResult with state {@value Status#FAILED} in case of destroy failure.
   * APIResult with state {@value Status#PARTIAL} in case of partial destroy.
   */
  @DELETE
  @Path("preparedqueries")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult destroyPreparedQueries(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @DefaultValue("") @QueryParam("user") String user) {
    checkSessionId(sessionid);
    int numDestroyed = 0;
    boolean failed = false;
    List<QueryPrepareHandle> handles = null;
    try {
      handles = getAllPreparedQueries(sessionid, user);
      for (QueryPrepareHandle prepared : handles) {
        if (destroyPrepared(sessionid, prepared)) {
          numDestroyed++;
        }
      }
    } catch (Exception e) {
      LOG.error("Error destroying prepared queries", e);
      failed = true;
    }
    String msgString = (StringUtils.isBlank(user) ? "" : " for user " + user);
    if (handles != null && numDestroyed == handles.size()) {
      return new APIResult(Status.SUCCEEDED, "Destroy all prepared "
          + "queries " + msgString + " is successful");
    } else {
      assert (failed);
      if (numDestroyed == 0) {
        return new APIResult(Status.FAILED, "Destroy all prepared "
            + "queries " + msgString + " has failed");        
      } else {
        return new APIResult(Status.PARTIAL, "Destroy all prepared "
            + "queries " + msgString +" is partial");        
      }
    }
  }

  private QueryHandle getQueryHandle(String queryHandle) {
    try {
      return QueryHandle.fromString(queryHandle);
    } catch (Exception e) {
      throw new BadRequestException("Invalid query handle: "  + queryHandle, e);
    }
  }

  private QueryPrepareHandle getPrepareHandle(String prepareHandle) {
    try {
      return QueryPrepareHandle.fromString(prepareHandle);
    } catch (Exception e) {
      throw new BadRequestException("Invalid prepared query handle: " + prepareHandle, e);
    }
  }

  /**
   * Get a prepared query specified by handle
   * 
   * @param sessionid The user session handle
   * @param prepareHandle The prepare handle
   * 
   * @return {@link GrillPreparedQuery}
   */
  @GET
  @Path("preparedqueries/{prepareHandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public GrillPreparedQuery getPreparedQuery(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("prepareHandle") String prepareHandle) {
    checkSessionId(sessionid);
    try {
      return queryServer.getPreparedQuery(sessionid,
          getPrepareHandle(prepareHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Destroy the prepared query specified by handle
   * 
   * @param sessionid The user session handle
   * @param prepareHandle The prepare handle
   * 
   * @return APIResult with state {@link Status#SUCCEEDED} in case of successful destroy.
   * APIResult with state {@link Status#FAILED} in case of destroy failure.
   */
  @DELETE
  @Path("preparedqueries/{prepareHandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult destroyPrepared(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("prepareHandle") String prepareHandle) {
    checkSessionId(sessionid);
    boolean ret = destroyPrepared(sessionid, getPrepareHandle(prepareHandle));
    if (ret) {
      return new APIResult(Status.SUCCEEDED, "Destroy on the query "
          + prepareHandle + " is successful");
    } else {
      return new APIResult(Status.FAILED, "Destroy on the query "
          + prepareHandle + " failed");        
    }
  }

  /**
   * Get grill query and its current status
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * 
   * @return {@link GrillQuery}
   */
  @GET
  @Path("queries/{queryHandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public GrillQuery getStatus(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryHandle") String queryHandle) {
    checkSessionId(sessionid);
    try {
      return queryServer.getQuery(sessionid,
          getQueryHandle(queryHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Cancel the query specified by the handle
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful cancellation.
   * APIResult with state {@value Status#FAILED} in case of cancellation failure.
   */
  @DELETE
  @Path("queries/{queryHandle}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult cancelQuery(@QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryHandle") String queryHandle) {
    checkSessionId(sessionid);
    boolean ret = cancelQuery(sessionid, getQueryHandle(queryHandle));
    if (ret) {
      return new APIResult(Status.SUCCEEDED, "Cancel on the query "
          + queryHandle + " is successful");
    } else {
      return new APIResult(Status.FAILED, "Cancel on the query "
          + queryHandle + " failed");        
    }
  }

  private boolean cancelQuery(GrillSessionHandle sessionid, QueryHandle queryHandle) {
    try {
      return queryServer.cancelQuery(sessionid, queryHandle);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  private boolean destroyPrepared(GrillSessionHandle sessionid, QueryPrepareHandle queryHandle) {
    try {
      return queryServer.destroyPrepared(sessionid, queryHandle);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Modify query configuration if it is not running yet.
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * @param conf The new configuration, will be on top of old one
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful update.
   * APIResult with state {@value Status#FAILED} in case of udpate failure.
   */
  @PUT
  @Path("queries/{queryHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult updateConf(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryHandle") String queryHandle, 
      @FormDataParam("conf") GrillConf conf) {
    checkSessionId(sessionid);
    try {
      boolean ret = queryServer.updateQueryConf(sessionid, getQueryHandle(queryHandle), conf);
      if (ret) {
        return new APIResult(Status.SUCCEEDED, "Update on the query conf for "
            + queryHandle + " is successful");
      } else {
        return new APIResult(Status.FAILED, "Update on the query conf for "
            + queryHandle + " failed");        
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Modify prepared query's configuration. This would be picked up for subsequent runs
   * of the prepared queries. The query wont be re-prepared with new configuration.
   * 
   * @param sessionid The user session handle
   * @param prepareHandle The prepare handle
   * @param conf The new configuration, will be on top of old one
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful update.
   * APIResult with state {@value Status#FAILED} in case of udpate failure.
   */
  @PUT
  @Path("preparedqueries/{prepareHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult updatePreparedConf(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("prepareHandle") String prepareHandle, 
      @FormDataParam("conf") GrillConf conf) {
    checkSessionId(sessionid);
    try {
      boolean ret = queryServer.updateQueryConf(sessionid, getPrepareHandle(prepareHandle), conf);
      if (ret) {
        return new APIResult(Status.SUCCEEDED, "Update on the query conf for "
            + prepareHandle + " is successful");
      } else {
        return new APIResult(Status.FAILED, "Update on the query conf for "
            + prepareHandle + " failed");        
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Submit prepared query for execution
   * 
   * @param sessionid The session in which user is submitting the query. Any
   *  configuration set in the session will be picked up.
   * @param prepareHandle The Query to run
   * @param operation The operation on the query. Supported operations are
   * {@value SubmitOp#EXECUTE} and {@value SubmitOp#EXECUTE_WITH_TIMEOUT}
   * @param conf The configuration for the execution of query
   * @param timeoutmillis The timeout for the query, honored only in case of
   *  {@value SubmitOp#EXECUTE_WITH_TIMEOUT} operation
   * 
   * @return {@link QueryHandle} in case of {@value SubmitOp#EXECUTE} operation.
   * {@link QueryHandleWithResultSet} in case {@value SubmitOp#EXECUTE_WITH_TIMEOUT} operation.
   */
  @POST
  @Path("preparedqueries/{prepareHandle}")
  @Consumes({MediaType.MULTIPART_FORM_DATA})
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QuerySubmitResult executePrepared(@FormDataParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("prepareHandle") String prepareHandle,
      @DefaultValue("EXECUTE") @FormDataParam("operation") String operation,
      @FormDataParam("conf") GrillConf conf,
      @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutmillis) {
    checkSessionId(sessionid);
    try {
      SubmitOp sop = null;
      try {
        sop = SubmitOp.valueOf(operation.toUpperCase());
      } catch (IllegalArgumentException e) {
      }
      if (sop == null) {
        throw new BadRequestException("Invalid operation type: " + operation +
            submitPreparedClue);
      }
      switch (sop) {
      case EXECUTE:
        return queryServer.executePrepareAsync(sessionid, getPrepareHandle(prepareHandle), conf);
      case EXECUTE_WITH_TIMEOUT:
        return queryServer.executePrepare(sessionid, getPrepareHandle(prepareHandle), timeoutmillis, conf);
      default:
        throw new BadRequestException("Invalid operation type: " + operation + submitPreparedClue);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get resultset metadata of the query
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * 
   * @return {@link QueryResultSetMetadata}
   */
  @GET
  @Path("queries/{queryHandle}/resultsetmetadata")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryResultSetMetadata getResultSetMetadata(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryHandle") String queryHandle) {
    checkSessionId(sessionid);
    try {
      return queryServer.getResultSetMetadata(sessionid, getQueryHandle(queryHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Fetch the result set
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * @param startIndex start index of the result
   * @param fetchSize fetch size
   * 
   * @return {@link QueryResult}
   */
  @GET
  @Path("queries/{queryHandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public QueryResult getResultSet(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryHandle") String queryHandle,
      @QueryParam("fromindex") long startIndex,
      @QueryParam("fetchsize") int fetchSize) {
    checkSessionId(sessionid);
    try {
      return queryServer.fetchResultSet(sessionid, getQueryHandle(queryHandle), startIndex, fetchSize);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get the http endpoint for result set
   *
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   *
   * @return Response with result as octet stream
   */
  @GET
  @Path("queries/{queryHandle}/httpresultset")
  @Produces({MediaType.APPLICATION_OCTET_STREAM})
  public Response getHttpResultSet(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryHandle") String queryHandle) {
    checkSessionId(sessionid);
    try {
      return queryServer.getHttpResultSet(sessionid, getQueryHandle(queryHandle));
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Close the result set once fetching is done
   * 
   * @param sessionid The user session handle
   * @param queryHandle The query handle
   * 
   * @return APIResult with state {@value Status#SUCCEEDED} in case of successful close.
   * APIResult with state {@value Status#FAILED} in case of close failure.
   */
  @DELETE
  @Path("queries/{queryHandle}/resultset")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public APIResult closeResultSet(
      @QueryParam("sessionid") GrillSessionHandle sessionid,
      @PathParam("queryHandle") String queryHandle){
    checkSessionId(sessionid);
    try {
      queryServer.closeResultSet(sessionid, getQueryHandle(queryHandle));
      return new APIResult(Status.SUCCEEDED, "Close on the result set"
          + " for query " + queryHandle + " is successful");

    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Create a named (saved) query. Once the query is created it can be invoked with using its handle.
   * Each invocation will create a new {@link com.inmobi.grill.api.query.GrillQuery}.
   * Named queries should be created with a query name, query string and configuration to be used
   * when executing the query
   *
   * @param sessionHandle
   * @param namedQuery
   * @return unique ID of the newly created named query
   */
  @POST
  @Path("/namedqueries")
  public String createNamedQuery(GrillSessionHandle sessionHandle, NamedQuery namedQuery) {
    checkSessionId(sessionHandle);
    try {
      return queryServer.createNamedQuery(sessionHandle, namedQuery);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Get a list of named queries matching the name argument
   * @param sessionHandle
   * @param queryName
   * @return
   */
  @GET
  @Path("/namedqueries/{queryName}")
  public NamedQueryList getNamedQueries(GrillSessionHandle sessionHandle,
                                          @PathParam("queryName") String queryName) {
    checkSessionId(sessionHandle);
    try {
      List<NamedQuery> namedQueries = queryServer.getNamedQueries(sessionHandle, queryName);
      if (namedQueries == null || namedQueries.isEmpty()) {
        throw new NotFoundException("No queries found matching the name: " + queryName);
      }
      return new NamedQueryList(namedQueries);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }

  /**
   * Update an existing named query
   * @param sessionHandle
   * @param namedQuery
   * @return
   */
  @PUT
  @Path("/namedqueries")
  public APIResult updateNamedQuery(GrillSessionHandle sessionHandle, NamedQuery namedQuery) {
    checkSessionId(sessionHandle);
    try {
      queryServer.updateNamedQuery(sessionHandle, namedQuery);
      return new APIResult(Status.SUCCEEDED, "Updated named query "+ namedQuery.getNamedQueryHandle());
    } catch (GrillException e) {
      return new APIResult(Status.FAILED, "Failed to update named query " + namedQuery.getNamedQueryHandle()
        + " reason: " + e.getMessage());
    }
  }

  /**
   * Delete an existing named query
   * @param sessionHandle
   * @param namedQueryHandle
   * @return
   */
  @DELETE
  @Path("/namedqueries/{namedQueryHandle}")
  public APIResult deleteNamedQuery(GrillSessionHandle sessionHandle,
                                    @PathParam("namedQueryHandle") String namedQueryHandle) {
    checkSessionId(sessionHandle);
    try {
      queryServer.deleteNamedQuery(sessionHandle, namedQueryHandle);
      return new APIResult(Status.SUCCEEDED, "Deleted named query " + namedQueryHandle);
    } catch (GrillException e) {
      return new APIResult(Status.FAILED, "Failed to delete named query " + namedQueryHandle
        + " reason: " + e.getMessage());
    }
  }

  /**
   * Execute a named query. Semantics of this call are similar to the {@link query()} call
   * @param sessionHandle
   * @param namedQueryHandle named query ID
   * @param operation to indicate if query should run in blocking or non blocking mode
   * @param timeoutMillis (applicable only for blocking mode) timeout to wait for query result
   * @return
   */
  @POST
  @Path("/namedqueries/execute")
  public QuerySubmitResult executeNamedQuery(GrillSessionHandle sessionHandle,
                                             @FormDataParam("namedQuery") String namedQueryHandle,
                                             @FormDataParam("operation") String operation,
                                             @DefaultValue("30000") @FormDataParam("timeoutmillis") Long timeoutMillis) {
    checkSessionId(sessionHandle);
    try {
      switch (getSubmitOp(operation)) {
        case EXECUTE:
          return queryServer.executeAsyncNamedQuery(sessionHandle, namedQueryHandle);
        case EXECUTE_WITH_TIMEOUT:
          return queryServer.executeNamedQuery(sessionHandle, namedQueryHandle, timeoutMillis);
        default:
          throw new BadRequestException("Not supported operation: " + operation);
      }
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    }
  }
}

