package ai.traceable.anomaly.config.service.global;

import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.global.validator.AnomalyGlobalConfigServiceValidator;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAllUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAllUnresolvedScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosResponse;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyGlobalConfigServiceImpl
    extends AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceImplBase {
  private final AnomalyGlobalConfigServiceValidator globalValidator;
  private final GlobalAnomalyConfigStatusManager anomalyConfigStatusManager;
  private final RuleInfoManager ruleInfoManager;

  @Inject
  public AnomalyGlobalConfigServiceImpl(
      AnomalyGlobalConfigServiceValidator globalValidator,
      GlobalAnomalyConfigStatusManager anomalyConfigStatusManager,
      RuleInfoManager ruleInfoManager) {
    this.globalValidator = globalValidator;
    this.anomalyConfigStatusManager = anomalyConfigStatusManager;
    this.ruleInfoManager = ruleInfoManager;
  }

  @Override
  public void getScopedAnomalyGlobalConfigStatus(
      GetScopedAnomalyGlobalConfigStatusRequest request,
      StreamObserver<GetScopedAnomalyGlobalConfigStatusResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Get Scoped Anomaly Global Config Status Request is not valid: {}",
          status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetScopedAnomalyGlobalConfigStatusResponse response =
          GetScopedAnomalyGlobalConfigStatusResponse.newBuilder()
              .setScopedConfig(
                  anomalyConfigStatusManager.getScopedAnomalyConfigStatus(
                      RequestContext.CURRENT.get(), request.getConfigScope()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllScopedAnomalyGlobalConfigStatus(
      GetAllScopedAnomalyGlobalConfigStatusRequest request,
      StreamObserver<GetAllScopedAnomalyGlobalConfigStatusResponse> responseObserver) {
    try {
      GetAllScopedAnomalyGlobalConfigStatusResponse response =
          GetAllScopedAnomalyGlobalConfigStatusResponse.newBuilder()
              .addAllScopedConfigs(
                  anomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
                      RequestContext.CURRENT.get()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getUnresolvedScopedAnomalyGlobalConfigStatus(
      GetUnresolvedScopedAnomalyGlobalConfigStatusRequest request,
      StreamObserver<GetUnresolvedScopedAnomalyGlobalConfigStatusResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Get Unresolved Scoped Anomaly Global Config Status Request is not valid: {}",
          status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetUnresolvedScopedAnomalyGlobalConfigStatusResponse response =
          GetUnresolvedScopedAnomalyGlobalConfigStatusResponse.newBuilder()
              .setScopedConfig(
                  anomalyConfigStatusManager.getUnresolvedScopedAnomalyConfigStatus(
                      RequestContext.CURRENT.get(), request.getConfigScope()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllUnresolvedScopedAnomalyGlobalConfigStatus(
      GetAllUnresolvedScopedAnomalyGlobalConfigStatusRequest request,
      StreamObserver<GetAllUnresolvedScopedAnomalyGlobalConfigStatusResponse> responseObserver) {
    try {
      GetAllUnresolvedScopedAnomalyGlobalConfigStatusResponse response =
          GetAllUnresolvedScopedAnomalyGlobalConfigStatusResponse.newBuilder()
              .addAllScopedConfigs(
                  anomalyConfigStatusManager.getAllUnresolvedScopedAnomalyConfigStatusConfigs(
                      RequestContext.CURRENT.get()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateScopedAnomalyGlobalConfigStatus(
      UpdateScopedAnomalyGlobalConfigStatusRequest request,
      StreamObserver<UpdateScopedAnomalyGlobalConfigStatusResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Update Scoped Anomaly Global Config Status Request is not valid: {}",
          status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      UpdateScopedAnomalyGlobalConfigStatusResponse response =
          UpdateScopedAnomalyGlobalConfigStatusResponse.newBuilder()
              .setScopedConfig(
                  anomalyConfigStatusManager.updateScopedAnomalyConfigStatus(
                      RequestContext.CURRENT.get(), request.getScopedConfig()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAnomalyRuleInfos(
      GetAnomalyRuleInfosRequest request,
      StreamObserver<GetAnomalyRuleInfosResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error("Get Anomaly Rules Info Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetAnomalyRuleInfosResponse response =
          GetAnomalyRuleInfosResponse.newBuilder()
              .addAllRuleInfos(
                  ruleInfoManager.getAnomalyRuleInfos(
                      RequestContext.CURRENT.get(),
                      request.getEventFamiliesList(),
                      request.getModsecRuleVersion()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteScopedAnomalyGlobalConfigStatus(
      DeleteScopedAnomalyGlobalConfigStatusRequest request,
      StreamObserver<DeleteScopedAnomalyGlobalConfigStatusResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error("Delete Anomaly Global Config Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      anomalyConfigStatusManager.deleteScopedAnomalyGlobalConfigStatus(
          RequestContext.CURRENT.get(), request.getConfigScope());
      responseObserver.onNext(DeleteScopedAnomalyGlobalConfigStatusResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
