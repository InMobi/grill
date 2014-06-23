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
 * Machine Learning service
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
   * Get a list of trainers available
   * @return
   */
  @GET
  @Path("trainers")
  public StringList getTrainerNames() {
    List<String> trainers = getMlService().getAlgorithms();
    StringList result = new StringList(trainers);
    return result;
  }

  /**
   * Get model ID list for a given algorithm
   * @param algorithm algorithm name
   * @return
   * @throws GrillException
   */
  @GET
  @Path("models/{algorithm}")
  public StringList getModelsForAlgo(@PathParam("algorithm") String algorithm) throws GrillException {
    List<String> models = getMlService().getModels(algorithm);
    if (models == null || models.isEmpty()) {
      throw new NotFoundException("No models found for algorithm " + algorithm);
    }
    return new StringList(models);
  }

  /**
   * Get metadata of the model given algorithm and model ID
   * @param algorithm algorithm name
   * @param modelID model ID
   * @return model metadata
   * @throws GrillException
   */
  @GET
  @Path("models/{algorithm}/{modelID}")
  public ModelMetadata getModelMetadata(@PathParam("algorithm") String algorithm,
                                         @PathParam("modelID") String modelID) throws GrillException {
    MLModel model = getMlService().getModel(algorithm, modelID);
    if (model == null) {
      throw new NotFoundException("Model not found " + modelID + ", algo=" + algorithm);
    }

    ModelMetadata meta = new ModelMetadata(
      model.getId(),
      model.getTable(),
      model.getTrainerName(),
      StringUtils.join(model.getParams(), ' '),
      model.getCreatedAt().toString(),
      getMlService().getModelPath(algorithm, modelID),
      model.getLabelColumn(),
      StringUtils.join(model.getFeatureColumns(), ",")
    );
    return meta;
  }

  /**
   * Delete a model given model ID and algorithm name
   * @param algorithm
   * @param modelID
   * @return confirmation text
   * @throws GrillException
   */
  @DELETE
  @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  @Path("models/{algorithm}/{modelID}")
  public String deleteModel(@PathParam("algorithm") String algorithm,
                              @PathParam("modelID") String modelID) throws GrillException {
    getMlService().deleteModel(algorithm, modelID);
    return "DELETED model=" + modelID + " algorithm=" + algorithm;
  }

  /**
   * Train a model given an algorithm name and algorithm parameters
   * <p>
   * Following parameters are mandatory and must be passed as part of the form
   *
   * <ol>
   *   <li>table - input Hive table to load training data from </li>
   *   <li>label - name of the labelled column</li>
   *   <li>feature - one entry per feature column. At least one feature column is required</li>
   * </ol>
   *
   * </p>
   * @param algorithm algorithm name
   * @param form form data
   * @return if model is successfully trained, the model ID will be returned
   * @throws GrillException
   */
  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Path("{algorithm}/train")
  public String train(@PathParam("algorithm") String algorithm,
                      MultivaluedMap<String, String> form) throws GrillException {

    // Check if trainer is valid
    if (getMlService().getTrainerForName(algorithm) == null) {
      throw new NotFoundException("Trainer for algo: " + algorithm + " not found");
    }

    if (isBlank(form.getFirst("table"))) {
      throw new BadRequestException("table parameter is rquired");
    }

    String table = form.getFirst("table");

    if (isBlank(form.getFirst("label"))) {
      throw new BadRequestException("label parameter is required");
    }

    // Check features
    List<String> featureNames = form.get("feature");
    if (featureNames.size() < 1) {
      throw new BadRequestException("At least one feature is required");
    }

    List<String> trainerArgs = new ArrayList<String>();
    Set<Map.Entry<String, List<String>>> paramSet = form.entrySet();

    for  (Map.Entry<String, List<String>> e : paramSet) {
      String p = e.getKey();
      List<String> values = e.getValue();
      if ("algorithm".equals(p) || "table".equals(p)) {
        continue;
      } else if ("feature".equals(p)) {
        for (String feature : values) {
          trainerArgs.add("feature");
          trainerArgs.add(feature);
        }
      } else if ("label".equals(p)) {
        trainerArgs.add("label");
        trainerArgs.add(values.get(0));
      } else {
        trainerArgs.add(p);
        trainerArgs.add(values.get(0));
      }
    }

    String modelId = getMlService().train(table, algorithm, trainerArgs.toArray(new String[]{}));
    LOG.info("Trained table " + table + " with algo " + algorithm
      + " params=" + trainerArgs.toString() + ", modelID=" + modelId);
    return modelId;
  }

  /**
   * Clear model cache (for admin use)
   * @return OK if the cache was cleared
   */
  @DELETE @Path("clearModelCache")
  @Produces(MediaType.TEXT_PLAIN)
  public Response clearModelCache() {
    ModelLoader.clearCache();
    LOG.info("Cleared model cache");
    return Response.ok("Cleared cache", MediaType.TEXT_PLAIN_TYPE).build();
  }

  /**
   * Run a test on a model for an algorithm
   * @param algorithm algorithm name
   * @param modelID model ID
   * @param table Hive table to run test on
   * @param session Grill session ID. This session ID will be used to run the test query
   * @return Test report ID
   * @throws GrillException
   */
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

  /**
   * Get list of reports for a given algorithm
   * @param algoritm
   * @return
   * @throws GrillException
   */
  @GET
  @Path("reports/{algorithm}")
  public StringList getReportsForAlgorithm(@PathParam("algorithm") String algoritm) throws GrillException {
    List<String> reports = getMlService().getTestReports(algoritm);
    if (reports == null || reports.isEmpty()) {
      throw new NotFoundException("No test reports found for " + algoritm);
    }
    return new StringList(reports);
  }


  /**
   * Get a single test report given the algorithm name and report id
   * @param algorithm
   * @param reportID
   * @return
   * @throws GrillException
   */
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

  /**
   * DELETE a report given the algorithm name and report ID
   * @param algorithm
   * @param reportID
   * @return
   * @throws GrillException
   */
  @DELETE
  @Path("reports/{algorithm}/{reportID}")
  @Consumes( {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN})
  public String deleteTestReport(@PathParam("algorithm") String algorithm,
                                  @PathParam("reportID") String reportID) throws GrillException {
    getMlService().deleteTestReport(algorithm, reportID);
    return "DELETED report="+ reportID +  " algorithm=" + algorithm;
  }
}
