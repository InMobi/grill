package com.inmobi.grill.server.query;

import com.google.common.base.Joiner;
import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.*;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.query.QueryExecutionService;
import com.inmobi.grill.server.session.SessionUIResource;
import org.glassfish.jersey.media.multipart.FormDataParam;

import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import java.io.UnsupportedEncodingException;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Created by arun on 25/7/14.
 */
@Path("/queryapi")
public class QueryServiceUIResource{

    public static final Log LOG = LogFactory.getLog(QueryServiceUIResource.class);

    private QueryExecutionService queryServer;

    private void checkQuery(String query) {
        if (StringUtils.isBlank(query)) {
            throw new BadRequestException("Invalid query");
        }
    }

    private void checkSessionId(GrillSessionHandle sessionHandle) {
        if (sessionHandle == null) {
            throw new BadRequestException("Invalid session handle");
        }
    }

    public QueryServiceUIResource() {
        LOG.info("Qery Servuce a");
        queryServer = (QueryExecutionService) GrillServices.get().getService("query");
    }

    private QueryHandle getQueryHandle(String queryHandle) {
        try {
            return QueryHandle.fromString(queryHandle);
        } catch (Exception e) {
            throw new BadRequestException("Invalid query handle: "  + queryHandle, e);
        }
    }

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

    @POST
    @Path("queries")
    @Consumes({MediaType.MULTIPART_FORM_DATA})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
    public QuerySubmitResult query(@FormDataParam("publicId") UUID publicId,
                                   @FormDataParam("query") String query) {
        GrillSessionHandle handle = SessionUIResource.openSessions.get(publicId);
        GrillConf conf;
        checkQuery(query);
        checkSessionId(handle);
        try {
            conf = new GrillConf();
            return queryServer.executeAsync(handle, query, conf);
        } catch (GrillException e) {
            throw new WebApplicationException(e);
        }
    }
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

    @GET
    @Path("queries/{queryHandle}/resultset")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
    public QueryResult getResultSet(
            @QueryParam("sessionid") GrillSessionHandle sessionid,
            @PathParam("queryHandle") String queryHandle,
            @QueryParam("pageNumber") int pageNumber,
            @QueryParam("fetchsize") int fetchSize) {
        checkSessionId(sessionid);
        try {
            return queryServer.fetchResultSet(sessionid, getQueryHandle(queryHandle), pageNumber * (fetchSize - 1),
                    fetchSize);
        } catch (GrillException e) {
            throw new WebApplicationException(e);
        }
    }

}
