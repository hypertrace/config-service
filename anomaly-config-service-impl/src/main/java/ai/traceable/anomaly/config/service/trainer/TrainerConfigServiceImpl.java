package ai.traceable.anomaly.config.service.trainer;

import ai.traceable.anomaly.config.service.trainer.trainingaction.TrainingActionManager;
import ai.traceable.anomaly.config.service.trainer.trainingaction.TrainingActionValidator;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigManager;
import ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigValidator;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteTrainingActionRequest;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteTrainingActionResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllTrainingActionsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllTrainingActionsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllUnresolvedScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllUnresolvedScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllUnresolvedVulnerabilityScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllUnresolvedVulnerabilityScopedTrainingConfigsResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.GetUnresolvedScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetUnresolvedScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigTypeSpecificFilter;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateVulnerabilityScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateVulnerabilityScopedTrainingConfigResponse;
import ai.traceable.anomaly.config.service.v1.trainer.UpsertTrainingActionRequest;
import ai.traceable.anomaly.config.service.v1.trainer.UpsertTrainingActionResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class TrainerConfigServiceImpl
    extends TrainerConfigServiceGrpc.TrainerConfigServiceImplBase {
  private final TrainingConfigValidator validator;
  private final TrainingActionValidator trainingActionValidator;
  private final TrainingConfigManager configManager;
  private final TrainingActionManager trainingActionManager;
  private final ScopedTrainingConfigConverter scopedTrainingConfigConverter;

  @Inject
  public TrainerConfigServiceImpl(
      TrainingConfigValidator validator,
      TrainingActionValidator trainingActionValidator,
      TrainingConfigManager configManager,
      TrainingActionManager trainingActionManager,
      ScopedTrainingConfigConverter scopedTrainingConfigConverter) {
    this.validator = validator;
    this.trainingActionValidator = trainingActionValidator;
    this.configManager = configManager;
    this.trainingActionManager = trainingActionManager;
    this.scopedTrainingConfigConverter = scopedTrainingConfigConverter;
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

  @Override
  public void updateVulnerabilityScopedTrainingConfig(
      UpdateVulnerabilityScopedTrainingConfigRequest request,
      StreamObserver<UpdateVulnerabilityScopedTrainingConfigResponse> responseObserver) {

    try {
      UpdateScopedTrainingConfigRequest convertedRequest =
          scopedTrainingConfigConverter.convert(request);
      Status status = validator.validate(convertedRequest);

      if (!status.isOk()) {
        log.error("UpdateScopedTrainingConfigRequest is not valid: {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }

      ScopedTrainingConfig updatedScopedTrainingConfig =
          configManager.updateScopedTrainingConfig(
              RequestContext.CURRENT.get(), convertedRequest.getScopedTrainingConfig());
      UpdateVulnerabilityScopedTrainingConfigResponse response =
          UpdateVulnerabilityScopedTrainingConfigResponse.newBuilder()
              .setScopedVulnerabilityConfig(
                  scopedTrainingConfigConverter.convert(updatedScopedTrainingConfig))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getUnresolvedScopedTrainingConfig(
      GetUnresolvedScopedTrainingConfigRequest request,
      StreamObserver<GetUnresolvedScopedTrainingConfigResponse> responseObserver) {
    Status status = validator.validate(request);
    if (!status.isOk()) {
      log.error(
          "GetUnresolvedScopedTrainingConfigRequest is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    try {
      GetUnresolvedScopedTrainingConfigResponse response =
          GetUnresolvedScopedTrainingConfigResponse.newBuilder()
              .setScopedTrainingConfig(
                  configManager.getUnresolvedTrainingConfig(
                      RequestContext.CURRENT.get(), request.getConfigScope(), request.getFilter()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getAllUnresolvedScopedTrainingConfig(
      GetAllUnresolvedScopedTrainingConfigRequest request,
      StreamObserver<GetAllUnresolvedScopedTrainingConfigResponse> responseObserver) {
    try {
      GetAllUnresolvedScopedTrainingConfigResponse response =
          GetAllUnresolvedScopedTrainingConfigResponse.newBuilder()
              .addAllScopedTrainingConfigs(
                  configManager.getAllUnresolvedTrainingConfig(
                      RequestContext.CURRENT.get(), request.getFilter()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void getAllUnresolvedVulnerabilityScopedTrainingConfigs(
      GetAllUnresolvedVulnerabilityScopedTrainingConfigsRequest request,
      StreamObserver<GetAllUnresolvedVulnerabilityScopedTrainingConfigsResponse> responseObserver) {
    try {
      List<ScopedTrainingConfig> vulnerabilityUnresolvedTrainingConfig =
          configManager.getAllUnresolvedTrainingConfig(
              RequestContext.CURRENT.get(),
              GetTrainingConfigsFilter.newBuilder()
                  .addTrainingConfigTypeSpecificFilter(
                      TrainingConfigTypeSpecificFilter.newBuilder()
                          .setVulnerabilityTrainingConfigFilter(
                              request.getVulnerabilityTrainingConfigFilter()))
                  .build());

      GetAllUnresolvedVulnerabilityScopedTrainingConfigsResponse response =
          GetAllUnresolvedVulnerabilityScopedTrainingConfigsResponse.newBuilder()
              .addAllScopedVulnerabilityTrainingConfigs(
                  scopedTrainingConfigConverter.convert(vulnerabilityUnresolvedTrainingConfig))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void deleteScopedTrainingConfig(
      DeleteScopedTrainingConfigRequest request,
      StreamObserver<DeleteScopedTrainingConfigResponse> responseObserver) {
    Status status = validator.validate(request);
    if (!status.isOk()) {
      log.error("DeleteScopedTrainingConfigRequest is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    try {
      ScopedTrainingConfig deletedScopedTrainingConfig =
          configManager.deleteTrainingConfig(
              RequestContext.CURRENT.get(),
              request.getScopedTrainingConfig(),
              request.getDeleteAnomalyConfigOption());
      responseObserver.onNext(
          DeleteScopedTrainingConfigResponse.newBuilder()
              .setDeletedScopedTrainingConfig(deletedScopedTrainingConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      log.error(exception.getMessage(), exception);
      responseObserver.onError(exception);
    }
  }

  @Override
  public void upsertTrainingAction(
      UpsertTrainingActionRequest request,
      StreamObserver<UpsertTrainingActionResponse> responseObserver) {
    Status status = trainingActionValidator.validate(request);

    if (!status.isOk()) {
      log.error("upsert training action request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      UpsertTrainingActionResponse response =
          UpsertTrainingActionResponse.newBuilder()
              .setScopedTrainingActionConfig(
                  trainingActionManager.upsertTrainingAction(
                      RequestContext.CURRENT.get(),
                      request.getConfigScope(),
                      request.getTrainingAction()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllTrainingActions(
      GetAllTrainingActionsRequest request,
      StreamObserver<GetAllTrainingActionsResponse> responseObserver) {
    try {
      GetAllTrainingActionsResponse response =
          GetAllTrainingActionsResponse.newBuilder()
              .addAllScopedTrainingActionConfigs(
                  trainingActionManager.getAllTrainingActions(RequestContext.CURRENT.get()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteTrainingAction(
      DeleteTrainingActionRequest request,
      StreamObserver<DeleteTrainingActionResponse> responseObserver) {
    try {
      trainingActionManager.deleteTrainingAction(
          RequestContext.CURRENT.get(), request.getConfigScope());
      responseObserver.onNext(DeleteTrainingActionResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
