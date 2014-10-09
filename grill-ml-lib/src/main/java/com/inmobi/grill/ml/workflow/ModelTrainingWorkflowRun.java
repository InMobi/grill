package com.inmobi.grill.ml.workflow;

import com.inmobi.grill.ml.MLModel;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Data about a single run of a model training workflow
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ModelTrainingWorkflowRun {

  public enum WorkflowStatus {
    SUCCESSFUL,
    FAILED,
    RUNNING
  };

  @Getter @Setter(value = AccessLevel.PROTECTED)
  WorkflowStatus status;

  @Getter @Setter(value = AccessLevel.PROTECTED)
  String workflowRunId;

  @Getter @Setter(value = AccessLevel.PROTECTED)
  long startTime;

  @Getter @Setter(value = AccessLevel.PROTECTED)
  long endTime;

  @Getter @Setter(value = AccessLevel.PROTECTED)
  MLModel trainedModel;
}
