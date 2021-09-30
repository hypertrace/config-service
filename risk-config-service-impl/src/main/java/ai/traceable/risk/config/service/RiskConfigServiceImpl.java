package ai.traceable.risk.config.service;

import ai.traceable.risk.config.service.factorgrid.RiskFactorGridConfigManager;
import ai.traceable.risk.config.service.factors.RiskFactorConfigsManager;
import ai.traceable.risk.config.service.level.RiskLevelConfigManager;
import ai.traceable.risk.config.service.v1.GetRiskImpactConfigsRequest;
import ai.traceable.risk.config.service.v1.GetRiskImpactConfigsResponse;
import ai.traceable.risk.config.service.v1.GetRiskLikelihoodConfigsRequest;
import ai.traceable.risk.config.service.v1.GetRiskLikelihoodConfigsResponse;
import ai.traceable.risk.config.service.v1.GetRiskScoringConfigsRequest;
import ai.traceable.risk.config.service.v1.GetRiskScoringConfigsResponse;
import ai.traceable.risk.config.service.v1.ResetRiskFactorGridConfigRequest;
import ai.traceable.risk.config.service.v1.ResetRiskFactorGridConfigResponse;
import ai.traceable.risk.config.service.v1.ResetRiskImpactConfigsRequest;
import ai.traceable.risk.config.service.v1.ResetRiskImpactConfigsResponse;
import ai.traceable.risk.config.service.v1.ResetRiskLevelConfigRequest;
import ai.traceable.risk.config.service.v1.ResetRiskLevelConfigResponse;
import ai.traceable.risk.config.service.v1.ResetRiskLikelihoodConfigsRequest;
import ai.traceable.risk.config.service.v1.ResetRiskLikelihoodConfigsResponse;
import ai.traceable.risk.config.service.v1.RiskConfigServiceGrpc;
import ai.traceable.risk.config.service.v1.UpdateRiskFactorGridConfigRequest;
import ai.traceable.risk.config.service.v1.UpdateRiskFactorGridConfigResponse;
import ai.traceable.risk.config.service.v1.UpdateRiskImpactConfigsRequest;
import ai.traceable.risk.config.service.v1.UpdateRiskImpactConfigsResponse;
import ai.traceable.risk.config.service.v1.UpdateRiskLevelConfigRequest;
import ai.traceable.risk.config.service.v1.UpdateRiskLevelConfigResponse;
import ai.traceable.risk.config.service.v1.UpdateRiskLikelihoodConfigsRequest;
import ai.traceable.risk.config.service.v1.UpdateRiskLikelihoodConfigsResponse;
import io.grpc.stub.StreamObserver;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RiskConfigServiceImpl extends RiskConfigServiceGrpc.RiskConfigServiceImplBase {

  private final RiskLevelConfigManager riskLevelConfigManager;
  private final RiskFactorGridConfigManager riskFactorGridConfigManager;
  private final RiskFactorConfigsManager riskFactorConfigsManager;

  @Inject
  public RiskConfigServiceImpl(
      RiskLevelConfigManager riskLevelConfigManager,
      RiskFactorGridConfigManager riskFactorGridConfigManager,
      RiskFactorConfigsManager riskFactorConfigsManager) {
    this.riskLevelConfigManager = riskLevelConfigManager;
    this.riskFactorGridConfigManager = riskFactorGridConfigManager;
    this.riskFactorConfigsManager = riskFactorConfigsManager;
  }

  @Override
  public void getRiskScoringConfigs(
      GetRiskScoringConfigsRequest request,
      StreamObserver<GetRiskScoringConfigsResponse> responseObserver) {
    try {
      responseObserver.onNext(
          GetRiskScoringConfigsResponse.newBuilder()
              .setRiskLevelConfig(
                  riskLevelConfigManager.getRiskLevelConfig(RequestContext.CURRENT.get()))
              .setRiskFactorGridConfig(
                  riskFactorGridConfigManager.getRiskFactorGridConfig(RequestContext.CURRENT.get()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateRiskLevelConfig(
      UpdateRiskLevelConfigRequest request,
      StreamObserver<UpdateRiskLevelConfigResponse> responseObserver) {
    try {
      responseObserver.onNext(
          UpdateRiskLevelConfigResponse.newBuilder()
              .setRiskLevelConfig(
                  riskLevelConfigManager.updateRiskLevelConfig(
                      RequestContext.CURRENT.get(), request.getRiskLevelConfigValues()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateRiskFactorGridConfig(
      UpdateRiskFactorGridConfigRequest request,
      StreamObserver<UpdateRiskFactorGridConfigResponse> responseObserver) {
    try {
      responseObserver.onNext(
          UpdateRiskFactorGridConfigResponse.newBuilder()
              .setRiskFactorGridConfig(
                  riskFactorGridConfigManager.updateRiskFactorGridConfig(
                      RequestContext.CURRENT.get(), request.getRiskFactorGridConfigValues()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void resetRiskLevelConfig(
      ResetRiskLevelConfigRequest request,
      StreamObserver<ResetRiskLevelConfigResponse> responseObserver) {
    try {
      responseObserver.onNext(
          ResetRiskLevelConfigResponse.newBuilder()
              .setRiskLevelConfig(
                  riskLevelConfigManager.resetRiskLevelConfig(RequestContext.CURRENT.get()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void resetRiskFactorGridConfig(
      ResetRiskFactorGridConfigRequest request,
      StreamObserver<ResetRiskFactorGridConfigResponse> responseObserver) {
    try {
      responseObserver.onNext(
          ResetRiskFactorGridConfigResponse.newBuilder()
              .setRiskFactorGridConfig(
                  riskFactorGridConfigManager.resetRiskFactorGridConfig(
                      RequestContext.CURRENT.get()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getRiskLikelihoodConfigs(
      GetRiskLikelihoodConfigsRequest request,
      StreamObserver<GetRiskLikelihoodConfigsResponse> responseObserver) {
    try {
      responseObserver.onNext(
          GetRiskLikelihoodConfigsResponse.newBuilder()
              .setRiskLikelihoodConfigs(
                  riskFactorConfigsManager.getRiskLikelihoodConfigs(RequestContext.CURRENT.get()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getRiskImpactConfigs(
      GetRiskImpactConfigsRequest request,
      StreamObserver<GetRiskImpactConfigsResponse> responseObserver) {
    try {
      responseObserver.onNext(
          GetRiskImpactConfigsResponse.newBuilder()
              .setRiskImpactConfigs(
                  riskFactorConfigsManager.getRiskImpactConfigs(RequestContext.CURRENT.get()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateRiskLikelihoodConfigs(
      UpdateRiskLikelihoodConfigsRequest request,
      StreamObserver<UpdateRiskLikelihoodConfigsResponse> responseObserver) {
    try {
      responseObserver.onNext(
          UpdateRiskLikelihoodConfigsResponse.newBuilder()
              .setRiskLikelihoodConfigs(
                  riskFactorConfigsManager.updateRiskLikelihoodConfigs(
                      RequestContext.CURRENT.get(), request.getRiskFactorConfigsList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateRiskImpactConfigs(
      UpdateRiskImpactConfigsRequest request,
      StreamObserver<UpdateRiskImpactConfigsResponse> responseObserver) {
    try {
      responseObserver.onNext(
          UpdateRiskImpactConfigsResponse.newBuilder()
              .setRiskImpactConfigs(
                  riskFactorConfigsManager.updateRiskImpactConfigs(
                      RequestContext.CURRENT.get(), request.getRiskFactorConfigsList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void resetRiskLikelihoodConfigs(
      ResetRiskLikelihoodConfigsRequest request,
      StreamObserver<ResetRiskLikelihoodConfigsResponse> responseObserver) {
    try {
      responseObserver.onNext(
          ResetRiskLikelihoodConfigsResponse.newBuilder()
              .setRiskLikelihoodConfigs(
                  riskFactorConfigsManager.resetRiskLikelihoodConfigs(
                      RequestContext.CURRENT.get(), request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void resetRiskImpactConfigs(
      ResetRiskImpactConfigsRequest request,
      StreamObserver<ResetRiskImpactConfigsResponse> responseObserver) {
    try {
      responseObserver.onNext(
          ResetRiskImpactConfigsResponse.newBuilder()
              .setRiskImpactConfigs(
                  riskFactorConfigsManager.resetRiskImpactConfigs(
                      RequestContext.CURRENT.get(), request.getFilter()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      responseObserver.onError(e);
    }
  }
}
