package com.inmobi.grill.server.ml;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.StringList;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.ml.MLService;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * ML Service resrouce
 */
@Path("/ml")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
@Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class MLServiceResource {

  MLService mlService;

  @Context
  HttpServletRequest request;

  private MLService getMlService() {
    if (mlService == null) {
      mlService = (MLService) GrillServices.get().getService(MLService.NAME);
    }
    return mlService;
  }

  /**
   *
   * @return
   */
  @GET
  @Path("/trainers")
  public StringList getTrainerNames() {
    List<String> trainers = getMlService().getTrainerNames();
    StringList result = new StringList(trainers);
    return result;
  }


  @POST
  @Path("/{algoName}/train")
  public String train(@PathParam("algoName") String algoName) throws GrillException {

    // Check if trainer is valid
    if (getMlService().getTrainerForName(algoName) == null) {
      throw new NotFoundException("Trainer for algo: " + algoName + " not found");
    }

    if (isBlank(request.getParameter("table"))) {
      throw new BadRequestException("table parameter is rquired");
    }

    String table = request.getParameter("table");

    if (isBlank(request.getParameter("l")) &&
        isBlank(request.getParameter("label"))) {
      throw new BadRequestException("label parameter is required");
    }

    // Check features
    String[] featureNames = request.getParameterValues("feature");
    if (featureNames.length < 1) {
      throw new BadRequestException("At least one feature is required");
    }

    List<String> trainerArgs = new ArrayList<String>();
    Enumeration paramNames = request.getParameterNames();

    while (paramNames.hasMoreElements()) {
      String p = (String) paramNames.nextElement();
      if ("algoName".equals(p) || "table".equals(p)) {
        continue;
      } else if ("feature".equals(p) || "f".equals(p)) {
        String[] features = request.getParameterValues(p);
        for (String feature : features) {
          trainerArgs.add("feature");
          trainerArgs.add(feature);
        }
      } else if ("label".equals(p) || "l".equals(p)) {
        trainerArgs.add("l");
        trainerArgs.add(request.getParameter(p));
      } else {
        trainerArgs.add(p);
        trainerArgs.add(request.getParameter(p));
      }
    }

    String modelId = getMlService().train(table, algoName, trainerArgs.toArray(new String[]{}));
    return modelId;
  }

}
