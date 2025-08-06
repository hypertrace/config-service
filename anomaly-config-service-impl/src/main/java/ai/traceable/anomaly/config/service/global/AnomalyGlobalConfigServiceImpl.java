package ai.traceable.anomaly.config.service.global;

import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.global.validator.AnomalyGlobalConfigServiceValidator;
import ai.traceable.anomaly.config.service.global.version.RuleVersionManager;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.global.DeleteRuleVersionConfigTypeRequest;
import ai.traceable.anomaly.config.service.v1.global.DeleteRuleVersionConfigTypeResponse;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAllScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAllUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAllUnresolvedScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosResponse;
import ai.traceable.anomaly.config.service.v1.global.GetAvailableRuleVersionsRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAvailableRuleVersionsResponse;
import ai.traceable.anomaly.config.service.v1.global.GetChangeLogDocRequest;
import ai.traceable.anomaly.config.service.v1.global.GetChangeLogDocResponse;
import ai.traceable.anomaly.config.service.v1.global.GetRulesChangeLogRequest;
import ai.traceable.anomaly.config.service.v1.global.GetRulesChangeLogResponse;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyGlobalConfigServiceImpl
    extends AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceImplBase {
  private final AnomalyGlobalConfigServiceValidator globalValidator;
  private final GlobalAnomalyConfigStatusManager anomalyConfigStatusManager;
  private final RuleInfoManager ruleInfoManager;
  private final RuleVersionManager ruleVersionManager;

  @Inject
  public AnomalyGlobalConfigServiceImpl(
      AnomalyGlobalConfigServiceValidator globalValidator,
      GlobalAnomalyConfigStatusManager anomalyConfigStatusManager,
      RuleInfoManager ruleInfoManager,
      RuleVersionManager ruleVersionManager) {
    this.globalValidator = globalValidator;
    this.anomalyConfigStatusManager = anomalyConfigStatusManager;
    this.ruleInfoManager = ruleInfoManager;
    this.ruleVersionManager = ruleVersionManager;
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
                      RequestContext.CURRENT.get(), request.getFilter().getApplicableScopesList()))
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
                      RequestContext.CURRENT.get(), request.getFilter().getApplicableScopesList()))
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
                      request.getEventFamiliesList(),
                      request.getModsecRuleVersion(),
                      request.getRuleTypeVersionsList(),
                      request.getUseTestModsecRules()))
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

  @Override
  public void deleteRuleVersionConfigType(
      DeleteRuleVersionConfigTypeRequest request,
      StreamObserver<DeleteRuleVersionConfigTypeResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error(
          "Delete Rule Version Config Type Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      ScopedAnomalyConfigStatusChange updatedConfig =
          anomalyConfigStatusManager.deleteRuleVersionConfigType(
              RequestContext.CURRENT.get(),
              request.getConfigScope(),
              request.getRuleVersionConfigTypesList(),
              request.getRuleType());
      responseObserver.onNext(
          DeleteRuleVersionConfigTypeResponse.newBuilder().setScopedConfig(updatedConfig).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAvailableRuleVersions(
      GetAvailableRuleVersionsRequest request,
      StreamObserver<GetAvailableRuleVersionsResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error("Get Available Rule Versions Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }
    try {
      GetAvailableRuleVersionsResponse response =
          GetAvailableRuleVersionsResponse.newBuilder()
              .setSupportedVersions(
                  ruleVersionManager.getAvailableRuleVersions(
                      request.getRuleType(), request.getFilter()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getRulesChangeLog(
      GetRulesChangeLogRequest request,
      StreamObserver<GetRulesChangeLogResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error("Get Rules Change Log Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetRulesChangeLogResponse response =
          GetRulesChangeLogResponse.newBuilder()
              .setRulesChangeLog(
                  ruleVersionManager.getRulesChangeLog(
                      request.getRuleType(),
                      request.getCurrentVersion(),
                      request.getPreviousVersion()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getChangeLogDoc(
      GetChangeLogDocRequest request, StreamObserver<GetChangeLogDocResponse> responseObserver) {
    Status status = globalValidator.validate(request);
    if (!status.isOk()) {
      log.error("Get Rules Change Log Doc Request is not valid: {}", status.getDescription());
      responseObserver.onError(status.asException());
      return;
    }

    try {
      GetChangeLogDocResponse response =
          GetChangeLogDocResponse.newBuilder()
              .setChangeLog(
                  ruleVersionManager.getChangeLogDoc(
                      request.getRuleType(),
                      request.getCurrentVersion(),
                      request.getPreviousVersion()))
              .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
