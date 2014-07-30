package com.inmobi.grill.server.metastore;

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

/**
 * Created by vikash pandey on 25/7/14.
 */

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.metastore.ObjectFactory;
import com.inmobi.grill.api.metastore.XCube;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.metastore.CubeMetastoreService;
import com.inmobi.grill.server.session.SessionUIResource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;
import java.util.*;

/**
 * metastore UI resource api
 *
 * This provides api for all things metastore UI.
 */

@Path("metastoreapi")
public class MetastoreUIResource {

    public static final Log LOG = LogFactory.getLog(MetastoreUIResource.class);
    public CubeMetastoreService getSvc() { return (CubeMetastoreService)GrillServices.get().getService("metastore");}

    private void checkSessionHandle(GrillSessionHandle sessionHandle) {
        if (sessionHandle == null) {
            throw new BadRequestException("Invalid session handle");
        }
    }

    /**
     * API to know if metastore service is up and running
     *
     * @return Simple text saying it up
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getMessage() {
        return "Metastore is up";
    }

    /*
     * Checks if a list of Strings contains a search word
     */
    private boolean checkAttributeMatching(List<String> attribList, String search)
    {
        Iterator<String> it = attribList.iterator();
        while(it.hasNext())
        {
            if(it.next().contains(search)) return true;
        }
        return false;
    }


    /**
     * Get all Cube names, Dimension Table names and Storage names
     *
     * @param publicId The publicId for the session in which user is working
     *
     * @return JSON string consisting of different table names and types
     *
     * @throws GrillException, JSONException
     */
    @GET @Path("tables")
    @Produces ({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public String showAllTables(@QueryParam("publicId") UUID publicId)
    {
        GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        JSONArray tableList = new JSONArray();
        List<String> cubes;
        try{
            cubes = getSvc().getAllCubeNames(sessionHandle);
        }
        catch(GrillException e){
            throw new WebApplicationException(e);
        }
        for(String cube : cubes)
        {
            try {
                tableList.put(new JSONObject().put("name", cube).put("type", "Cube"));
            }
            catch(JSONException j){
                LOG.error(j);
            }
        }
        List<String> dimTables;
        try{
            dimTables = getSvc().getAllDimTableNames(sessionHandle);
        }
        catch(GrillException e){
            throw new WebApplicationException(e);
        }
        for(String dimTable : dimTables)
        {
            try {
                tableList.put(new JSONObject().put("name", dimTable).put("type", "DimensionTable"));
            }
            catch(JSONException j){
                LOG.error(j);
            }
        }
        List<String> storageTables;
        try{
            storageTables = getSvc().getAllStorageNames(sessionHandle);
        }
        catch(GrillException e){
            throw new WebApplicationException(e);
        }
        for(String storageTable : storageTables)
        {
            try {
                tableList.put(new JSONObject().put("name", storageTable).put("type", "StorageTable"));
            }
            catch(JSONException j){
                LOG.error(j);
            }
        }
        return tableList.toString();
    }



    /**
     * Get all Table names and types which contain the search word
     *
     * @param publicId The publicId for the session in which user is working
     *
     * @param keyword keyword to search
     *
     * @return JSON string consisting of different table names and types
     *
     * @throws GrillException, JSONException
     */
    @GET @Path("tables/{keyword}")
    @Produces ({MediaType.TEXT_HTML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public String showFilterResults(@QueryParam("publicId") UUID publicId, @PathParam("keyword") String keyword)
    {
        GrillSessionHandle sessionHandle = SessionUIResource.openSessions.get(publicId);
        checkSessionHandle(sessionHandle);
        JSONArray tableList= null;
        JSONArray searchResults = new JSONArray();
        try {

             tableList = new JSONArray(showAllTables(publicId));
        }catch(JSONException j)
        {
            LOG.error(j);
        }
        for(int item = 0; item < tableList.length(); item++)
        {
            String name =null, type=null;
            try {
                name = tableList.getJSONObject(item).getString("name");
                type = tableList.getJSONObject(item).getString("type");
            }catch(JSONException j)
            {
                LOG.error(j);
            }
            if(name.contains(keyword)) {
                try{
                    searchResults.put(new JSONObject().put("name", name).put("type", type));
                }catch(JSONException j)
                {
                    LOG.error(j);
                }
            }
            /*else if (type.equals("Cube")) {
                XCube cube;
                try {
                    cube = getSvc().getCube(sessionHandle, name);
                    LOG.info("GOT THE CUBE");
                } catch (GrillException e) {
                    throw new WebApplicationException(e);
                }
                List<String> cubeList = cube.getDimAttrNames().getDimAttrNames();
                LOG.info(" DIMATTRNAMES ");

                if ((checkAttributeMatching(cube.getDimAttrNames().getDimAttrNames(), keyword)) || (checkAttributeMatching(cube.getMeasureNames().getMeasures(), keyword))) {
                    try{
                        searchResults.put(new JSONObject().put("name", name).put("type", type));
                    }catch(JSONException j)
                    {
                    LOG.info(j);
                    }
                }
            }*/
        }
        return searchResults.toString();
    }
}
