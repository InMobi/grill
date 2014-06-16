package com.inmobi.grill.server.ml;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.StringList;
import com.inmobi.grill.api.ml.ModelMetadata;
import com.inmobi.grill.api.ml.TestReport;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.ml.MLModel;
import com.inmobi.grill.server.api.ml.MLService;
import com.inmobi.grill.server.api.ml.MLTestReport;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.*;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * ML Service resrouce
 */
@Path("/ml")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class MLServiceResource {
  public static final Log LOG = LogFactory.getLog(MLServiceResource.class);
  MLService mlService;


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
  @Path("trainers")
  public StringList getTrainerNames() {
    List<String> trainers = getMlService().getAlgorithms();
    StringList result = new StringList(trainers);
    return result;
  }

  @GET
  @Path("models/{algoName}")
  public StringList getModelsForAlgo(@PathParam("algoName") String algoName) throws GrillException {
    return new StringList(getMlService().getModels(algoName));
  }

  @GET
  @Path("models/{algoName}/{modelID}")
  public ModelMetadata getModelMetadata(@PathParam("algoName") String algoName,
                                         @PathParam("modelID") String modelID) throws GrillException {
    MLModel model = getMlService().getModel(algoName, modelID);
    if (model == null) {
      throw new NotFoundException("Model not found " + modelID + ", algo=" + algoName);
    }

    ModelMetadata meta = new ModelMetadata(
      model.getId(),
      model.getTable(),
      model.getTrainerName(),
      StringUtils.join(model.getParams(), ' '),
      model.getCreatedAt().toString(),
      getMlService().getModelPath(algoName, modelID),
      model.getLabelColumn(),
      StringUtils.join(model.getFeatureColumns(), ",")
    );
    return meta;
  }

  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Path("{algoName}/train")
  public String train(@PathParam("algoName") String algoName,
                      MultivaluedMap<String, String> form) throws GrillException {

    // Check if trainer is valid
    if (getMlService().getTrainerForName(algoName) == null) {
      throw new NotFoundException("Trainer for algo: " + algoName + " not found");
    }

    if (isBlank(form.getFirst("table"))) {
      throw new BadRequestException("table parameter is rquired");
    }

    String table = form.getFirst("table");

    if (isBlank(form.getFirst("-label"))) {
      throw new BadRequestException("label parameter is required");
    }

    // Check features
    List<String> featureNames = form.get("-feature");
    if (featureNames.size() < 1) {
      throw new BadRequestException("At least one feature is required");
    }

    List<String> trainerArgs = new ArrayList<String>();
    Set<Map.Entry<String, List<String>>> paramSet = form.entrySet();

    for  (Map.Entry<String, List<String>> e : paramSet) {
      String p = e.getKey();
      List<String> values = e.getValue();
      if ("algoName".equals(p) || "table".equals(p)) {
        continue;
      } else if ("-feature".equals(p)) {
        for (String feature : values) {
          trainerArgs.add("-feature");
          trainerArgs.add(feature);
        }
      } else if ("-label".equals(p)) {
        trainerArgs.add("-label");
        trainerArgs.add(values.get(0));
      } else {
        trainerArgs.add(p);
        trainerArgs.add(values.get(0));
      }
    }

    String modelId = getMlService().train(table, algoName, trainerArgs.toArray(new String[]{}));
    LOG.info("Trained table " + table + " with algo " + algoName
      + " params=" + trainerArgs.toString() + ", modelID=" + modelId);
    return modelId;
  }

  @DELETE @Path("clearModelCache")
  @Produces(MediaType.TEXT_PLAIN)
  public Response clearModelCache() {
    ModelLoader.clearCache();
    LOG.info("Cleared model cache");
    return Response.ok("Cleared cache", MediaType.TEXT_PLAIN_TYPE).build();
  }

  @POST
  @Path("test/{table}/{algorithm}/{modelID}")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public String test(@PathParam("algorithm") String algorithm,
                     @PathParam("modelID") String modelID,
                     @PathParam("table") String table,
                     @FormDataParam("sessionid") GrillSessionHandle session) throws GrillException {
    MLTestReport testReport = getMlService().testModel(session, table, algorithm, modelID);
    return testReport.getReportID();
  }

  @GET
  @Path("reports/{algorithm}")
  public StringList getReportsForAlgorithm(@PathParam("algorithm") String algoritm) throws GrillException {
    List<String> reports = getMlService().getTestReports(algoritm);
    if (reports == null || reports.isEmpty()) {
      throw new NotFoundException("No test reports found for " + algoritm);
    }
    return new StringList(reports);
  }


  @GET
  @Path("reports/{algorithm}/{reportID}")
  public TestReport getTestReport(@PathParam("algorithm") String algorithm,
                                  @PathParam("reportID") String reportID) throws GrillException {
    MLTestReport report = getMlService().getTestReport(algorithm, reportID);

    if (report == null) {
      throw new NotFoundException("Test report: " + reportID + " not found for algorithm " + algorithm);
    }

    TestReport result = new TestReport(
      report.getTestTable(),
      report.getTestOutputPath(),
      report.getPredictionResultColumn(),
      report.getLabelColumn(),
      StringUtils.join(report.getFeatureColumns(), ","),
      report.getAlgorithm(),
      report.getModelID(),
      report.getReportID(),
      report.getGrillQueryID()
    );
    return result;
  }

}
