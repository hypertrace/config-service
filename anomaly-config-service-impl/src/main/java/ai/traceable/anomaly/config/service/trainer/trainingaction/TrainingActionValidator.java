package ai.traceable.anomaly.config.service.trainer.trainingaction;

import ai.traceable.anomaly.config.service.v1.trainer.GetAllTrainingActionsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingActionRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpsertTrainingActionRequest;
import io.grpc.Status;

public interface TrainingActionValidator {
  Status validate(UpsertTrainingActionRequest request);

  Status validate(GetAllTrainingActionsRequest request);

  Status validate(GetTrainingActionRequest request);
}
