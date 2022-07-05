package ai.traceable.anomaly.config.service.global;

import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.global.status.ConfigStatusManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.global.validator.AnomalyGlobalConfigServiceValidator;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosResponse;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateAnomalyGlobalConfigStatusResponse;
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
  private final ConfigStatusManager configStatusManager;
  private final GlobalAnomalyConfigStatusManager anomalyConfigStatusManager;
  private final RuleInfoManager ruleInfoManager;

  @Inject
  public AnomalyGlobalConfigServiceImpl(
      AnomalyGlobalConfigServiceValidator globalValidator,
      ConfigStatusManager configStatusManager,
      GlobalAnomalyConfigStatusManager anomalyConfigStatusManager,
      RuleInfoManager ruleInfoManager) {
    this.globalValidator = globalValidator;
    this.configStatusManager = configStatusManager;
    this.anomalyConfigStatusManager = anomalyConfigStatusManager;
    this.ruleInfoManager = ruleInfoManager;
  }

  @Override
  public void getAnomalyGlobalConfigStatus(
      GetAnomalyGlobalConfigStatusRequest request,
      StreamObserver<GetAnomalyGlobalConfigStatusResponse> responseObserver) {

    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Get Anomaly Global Config Status Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetAnomalyGlobalConfigStatusResponse response =
          GetAnomalyGlobalConfigStatusResponse.newBuilder()
              .setConfigStatus(
                  configStatusManager.getAnomalyConfigStatus(
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
  public void updateAnomalyGlobalConfigStatus(
      UpdateAnomalyGlobalConfigStatusRequest request,
      StreamObserver<UpdateAnomalyGlobalConfigStatusResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Update Anomaly Global Config Status Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      UpdateAnomalyGlobalConfigStatusResponse response =
          UpdateAnomalyGlobalConfigStatusResponse.newBuilder()
              .setConfigStatus(
                  configStatusManager.updateAnomalyConfigStatus(
                      RequestContext.CURRENT.get(),
                      request.getConfigScope(),
                      request.getConfigStatus()))
              .build();
      // temporary dual-write
      anomalyConfigStatusManager.updateScopedAnomalyConfigStatus(
          RequestContext.CURRENT.get(),
          ScopedAnomalyConfigStatusChange.newBuilder()
              .setConfigScope(request.getConfigScope())
              .setConfigStatus(response.getConfigStatus())
              .build());

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
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
                      RequestContext.CURRENT.get(), request.getEventFamiliesList()))
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
