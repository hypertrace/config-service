package ai.traceable.anomaly.config.service.trainer;

import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigManager;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigValidator;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class TrainerConfigServiceImpl
    extends TrainerConfigServiceGrpc.TrainerConfigServiceImplBase {
  private final TrainingConfigValidator validator;
  private final TrainingConfigManager configManager;

  @Inject
  public TrainerConfigServiceImpl(
      TrainingConfigValidator validator, TrainingConfigManager configManager) {
    this.validator = validator;
    this.configManager = configManager;
  }

  @Override
  public void getScopedTrainingConfig(
      GetScopedTrainingConfigRequest request,
      StreamObserver<GetScopedTrainingConfigResponse> responseObserver) {
    Status status = validator.validate(request);

    if (!status.isOk()) {
      log.error("GetScopedTrainingConfigRequest is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetScopedTrainingConfigResponse response =
          GetScopedTrainingConfigResponse.newBuilder()
              .setScopedTrainingConfig(
                  configManager.getScopedTrainingConfig(
                      RequestContext.CURRENT.get(), request.getConfigScope(), request.getFilter()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllScopedTrainingConfigs(
      GetAllScopedTrainingConfigsRequest request,
      StreamObserver<GetAllScopedTrainingConfigsResponse> responseObserver) {
    try {
      GetAllScopedTrainingConfigsResponse response =
          GetAllScopedTrainingConfigsResponse.newBuilder()
              .addAllScopedTrainingConfigs(
                  configManager.getAllScopedTrainingConfig(
                      RequestContext.CURRENT.get(), request.getFilter()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateScopedTrainingConfig(
      UpdateScopedTrainingConfigRequest request,
      StreamObserver<UpdateScopedTrainingConfigResponse> responseObserver) {
    Status status = validator.validate(request);

    if (!status.isOk()) {
      log.error("UpdateScopedTrainingConfigRequest is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      UpdateScopedTrainingConfigResponse response =
          UpdateScopedTrainingConfigResponse.newBuilder()
              .setScopedTrainingConfig(
                  configManager.updateScopedTrainingConfig(
                      RequestContext.CURRENT.get(), request.getScopedTrainingConfig()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
