package ai.traceable.anomaly.config.service.detector;

import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigValidator;
import ai.traceable.anomaly.config.service.detector.migration.AnomalyDetectionMigrationManager;
import ai.traceable.anomaly.config.service.v1.detector.DeleteScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.DeleteScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.detector.GetAllGlobalResolvedScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAllGlobalResolvedScopedAnomalyDetectionConfigsResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetAllScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAllScopedAnomalyDetectionConfigsResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetAllUnresolvedScopedAnomalyDetectionConfigsRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetAllUnresolvedScopedAnomalyDetectionConfigsResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetGlobalResolvedScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetGlobalResolvedScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.GetUnresolvedScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetUnresolvedScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigResponse;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class DetectorConfigServiceImpl
    extends DetectorConfigServiceGrpc.DetectorConfigServiceImplBase {
  private final AnomalyDetectionConfigValidator configValidator;
  private final AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private final AnomalyDetectionMigrationManager anomalyDetectionMigrationManager;

  @Inject
  public DetectorConfigServiceImpl(
      AnomalyDetectionConfigValidator configValidator,
      AnomalyDetectionConfigManager anomalyDetectionConfigManager,
      AnomalyDetectionMigrationManager anomalyDetectionMigrationManager) {
    this.configValidator = configValidator;
    this.anomalyDetectionConfigManager = anomalyDetectionConfigManager;
    this.anomalyDetectionMigrationManager = anomalyDetectionMigrationManager;
  }

  @Override
  public void getScopedAnomalyDetectionConfig(
      GetScopedAnomalyDetectionConfigRequest request,
      StreamObserver<GetScopedAnomalyDetectionConfigResponse> responseObserver) {
    Status status = configValidator.validate(request);

    if (!status.isOk()) {
      log.error("GetScopedAnomalyDetectionConfigRequest is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    anomalyDetectionMigrationManager.migrateToApiProtectIfApplicable(RequestContext.CURRENT.get());
    try {
      GetScopedAnomalyDetectionConfigResponse response =
          GetScopedAnomalyDetectionConfigResponse.newBuilder()
              .setScopedAnomalyDetectionConfig(
                  anomalyDetectionConfigManager.getScopedAnomalyDetectionConfig(
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
  public void getGlobalResolvedScopedAnomalyDetectionConfig(
      GetGlobalResolvedScopedAnomalyDetectionConfigRequest request,
      StreamObserver<GetGlobalResolvedScopedAnomalyDetectionConfigResponse> responseObserver) {
    Status status = configValidator.validate(request);

    if (!status.isOk()) {
      log.error(
          "GetGlobalResolvedScopedAnomalyDetectionConfigRequest is not valid: {}",
          status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    anomalyDetectionMigrationManager.migrateToApiProtectIfApplicable(RequestContext.CURRENT.get());
    try {
      GetGlobalResolvedScopedAnomalyDetectionConfigResponse response =
          GetGlobalResolvedScopedAnomalyDetectionConfigResponse.newBuilder()
              .setScopedAnomalyDetectionConfig(
                  anomalyDetectionConfigManager.getGlobalResolvedScopedAnomalyDetectionConfig(
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
  public void getAllGlobalResolvedScopedAnomalyDetectionConfigs(
      GetAllGlobalResolvedScopedAnomalyDetectionConfigsRequest request,
      StreamObserver<GetAllGlobalResolvedScopedAnomalyDetectionConfigsResponse> responseObserver) {
    anomalyDetectionMigrationManager.migrateToApiProtectIfApplicable(RequestContext.CURRENT.get());
    try {
      GetAllGlobalResolvedScopedAnomalyDetectionConfigsResponse response =
          GetAllGlobalResolvedScopedAnomalyDetectionConfigsResponse.newBuilder()
              .addAllScopedAnomalyDetectionConfigs(
                  anomalyDetectionConfigManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(
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
  public void getAllScopedAnomalyDetectionConfigs(
      GetAllScopedAnomalyDetectionConfigsRequest request,
      StreamObserver<GetAllScopedAnomalyDetectionConfigsResponse> responseObserver) {
    anomalyDetectionMigrationManager.migrateToApiProtectIfApplicable(RequestContext.CURRENT.get());
    try {
      GetAllScopedAnomalyDetectionConfigsResponse response =
          GetAllScopedAnomalyDetectionConfigsResponse.newBuilder()
              .addAllScopedAnomalyDetectionConfigs(
                  anomalyDetectionConfigManager.getAllScopedAnomalyDetectionConfig(
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
  public void getUnresolvedScopedAnomalyDetectionConfig(
      GetUnresolvedScopedAnomalyDetectionConfigRequest request,
      StreamObserver<GetUnresolvedScopedAnomalyDetectionConfigResponse> responseObserver) {
    Status status = configValidator.validate(request);

    if (!status.isOk()) {
      log.error(
          "GetUnresolvedScopedAnomalyDetectionConfigRequest is not valid: {}",
          status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    anomalyDetectionMigrationManager.migrateToApiProtectIfApplicable(RequestContext.CURRENT.get());
    try {
      GetUnresolvedScopedAnomalyDetectionConfigResponse response =
          GetUnresolvedScopedAnomalyDetectionConfigResponse.newBuilder()
              .setScopedAnomalyDetectionConfig(
                  anomalyDetectionConfigManager.getUnresolvedScopedAnomalyDetectionConfig(
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
  public void getAllUnresolvedScopedAnomalyDetectionConfigs(
      GetAllUnresolvedScopedAnomalyDetectionConfigsRequest request,
      StreamObserver<GetAllUnresolvedScopedAnomalyDetectionConfigsResponse> responseObserver) {
    anomalyDetectionMigrationManager.migrateToApiProtectIfApplicable(RequestContext.CURRENT.get());
    try {
      GetAllUnresolvedScopedAnomalyDetectionConfigsResponse response =
          GetAllUnresolvedScopedAnomalyDetectionConfigsResponse.newBuilder()
              .addAllScopedAnomalyDetectionConfigs(
                  anomalyDetectionConfigManager.getAllUnresolvedScopedAnomalyDetectionConfigs(
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
  public void deleteScopedAnomalyDetectionConfig(
      DeleteScopedAnomalyDetectionConfigRequest request,
      StreamObserver<DeleteScopedAnomalyDetectionConfigResponse> responseObserver) {
    Status status = configValidator.validate(request);

    if (!status.isOk()) {
      log.error(
          "DeleteScopedAnomalyDetectionConfigRequest is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      ScopedAnomalyDetectionConfig deletedScopedAnomalyDetectionConfig =
          anomalyDetectionConfigManager.deleteScopedAnomalyDetectionConfig(
              RequestContext.CURRENT.get(),
              request.getScopedAnomalyDetectionConfig(),
              request.getDeleteAnomalyConfigOption());
      responseObserver.onNext(
          DeleteScopedAnomalyDetectionConfigResponse.newBuilder()
              .setDeletedScopedAnomalyDetectionConfig(deletedScopedAnomalyDetectionConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateScopedAnomalyDetectionConfig(
      UpdateScopedAnomalyDetectionConfigRequest request,
      StreamObserver<UpdateScopedAnomalyDetectionConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();

    try {
      Status status = configValidator.validate(request, requestContext);

      if (!status.isOk()) {
        log.error(
            "UpdateScopedAnomalyDetectionConfigRequest is not valid: {}", status.getDescription());
        responseObserver.onError(status.asException());
        return;
      }

      UpdateScopedAnomalyDetectionConfigResponse response =
          UpdateScopedAnomalyDetectionConfigResponse.newBuilder()
              .setScopedAnomalyDetectionConfig(
                  anomalyDetectionConfigManager.updateScopedAnomalyDetectionConfig(
                      requestContext, request.getScopedAnomalyDetectionConfig()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
