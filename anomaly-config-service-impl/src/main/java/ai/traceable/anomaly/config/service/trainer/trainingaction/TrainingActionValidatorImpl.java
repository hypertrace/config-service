package ai.traceable.anomaly.config.service.trainer.trainingaction;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllTrainingActionsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdFamily;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction;
import ai.traceable.anomaly.config.service.v1.trainer.UpsertTrainingActionRequest;
import io.grpc.Status;
import javax.inject.Inject;

public class TrainingActionValidatorImpl implements TrainingActionValidator {

  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  public TrainingActionValidatorImpl(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  @Override
  public Status validate(UpsertTrainingActionRequest request) {
    // validate config scope
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "UpsertTrainingActionRequest should have a valid config scope");
    }
    Status status = anomalyConfigValidator.validate(request.getConfigScope());
    if (status != Status.OK) {
      return status;
    }

    // validate training action
    if (!request.hasTrainingAction()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "UpsertTrainingActionRequest should have a valid training action");
    }
    return validateTrainingAction(request.getTrainingAction());
  }

  @Override
  public Status validate(GetAllTrainingActionsRequest request) {
    return Status.OK;
  }

  private Status validateTrainingAction(TrainingAction trainingAction) {
    switch (trainingAction.getActionCase()) {
      case ACTION_NOT_SET:
        return Status.INVALID_ARGUMENT.withDescription("TrainingAction should have a valid action");
      case FORCE_LEARN_ACTION:
        if (trainingAction.getForceLearnAction().getThresholdFamily()
            == ThresholdFamily.THRESHOLD_FAMILY_UNSPECIFIED) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Force learn action should have a valid threshold family");
        }
      default:
        return Status.OK;
    }
  }
}
