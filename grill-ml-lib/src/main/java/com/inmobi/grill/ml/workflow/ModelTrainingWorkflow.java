package com.inmobi.grill.ml.workflow;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.ml.MLModel;
import com.inmobi.grill.ml.spark.TableTrainingSpec;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

public class ModelTrainingWorkflow implements Callable<ModelTrainingWorkflowRun> {

  // Auto generated primary key
  @Getter
  private final String workFlowId;

  // Information needed to create training RDD such as label & feature columns, table name
  // DB name etc
  @Getter @Setter
  private TableTrainingSpec trainingSpec;

  // Algorithm used to generate the trainer
  @Getter
  private String algorithm;

  // Frequency for scheduling the workflow
  @Getter @Setter
  private WorkflowFrequency frequency;

  @Getter
  private final String userName;


  private ModelTrainingWorkflow(String workFlowId,
                               TableTrainingSpec trainingSpec,
                               String algorithm,
                               WorkflowFrequency frequency,
                               String userName) {
    this.workFlowId = workFlowId;
    this.algorithm = algorithm;
    this.frequency = frequency;
    this.trainingSpec = trainingSpec;
    this.userName = userName;
  }

  /**
   * Create the workflow with auto-generated id
   */
  public static ModelTrainingWorkflow createWorkflow(TableTrainingSpec trainingSpec,
                                                     String algorithm,
                                                     WorkflowFrequency frequency) throws GrillException {
    //FIXME
    return null;
  }

  /**
   * Get workflow instance by ID
   * @param workFlowId
   * @return
   */
  public static ModelTrainingWorkflow findById(String workFlowId) {
    return null;
  }

  /**
   * Get workflows of a user
   * @param userName
   * @return
   */
  public static List<ModelTrainingWorkflow> findByUser(String userName) {
    //FIXME
    return null;
  }

  /**
   * Update certain attributes of the workflow and persist to DB
   * @param workflow
   */
  public static void save(ModelTrainingWorkflow workflow) {
    //FIXME
  }

  /**
   * Run a single instance of the workflow. Results in a trained MLModel
   * @return
   * @throws Exception
   */
  @Override
  public ModelTrainingWorkflowRun call() throws Exception {
    //FIXME Get appropriate GrillML instance to train the model
    return null;
  }

  /**
   * Iterate through all models created by the workflow
   * @param order ascending or descending order of model creation date
   * @param limit number of models to return
   * @return
   */
  public Iterator<MLModel> getAllModels(boolean order, int limit) {
    //FIXME
    return null;
  }

  /**
   * Get model by position
   * Get latest model: getModelByIndex(0)
   * Get previous model: getModelByIndex(1)
   * Get nth last model: getModelByIndex(n)
   * @param index index before the current model
   * @return
   */
  public MLModel getModelByIndex(int index) {
    //FIXME
    return null;
  }

  public MLModel getLatestModel() {
    return getModelByIndex(0);
  }

  /**
   * Get run information of the workflow between given dates
   * @param fromDate lower bound for the date range
   * @param toDate upper end of the date range, defaults to Calendar.getTime()
   * @param limit number of runs to return
   * @return
   */
  public List<ModelTrainingWorkflowRun> getPastWorkflowRuns(Date fromDate,
                                                            Date toDate,
                                                            int limit) throws GrillException {
    //FIXME
    return null;
  }

  /**
   * Get status of a workflow run
   * @param workflowRunId
   * @return
   * @throws GrillException
   */
  public ModelTrainingWorkflowRun getWorkflowRunStatus(String workflowRunId)
    throws GrillException {
    //FIXME
    return null;
  }
}
