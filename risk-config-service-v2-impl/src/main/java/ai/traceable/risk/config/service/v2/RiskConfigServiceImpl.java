package ai.traceable.risk.config.service.v2;

import ai.traceable.risk.config.service.v2.contributors.validator.RiskContributorConfigsValidator;
import ai.traceable.risk.config.service.v2.factors.manager.RiskFactorConfigsManager;
import ai.traceable.risk.config.service.v2.grid.manager.RiskScoringGridConfigManager;
import ai.traceable.risk.config.service.v2.grid.validator.RiskScoringGridConfigValidator;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskConfigServiceImpl extends RiskConfigServiceGrpc.RiskConfigServiceImplBase {

  private final RiskLevelConfigValues defaultRiskLevelConfigValues;
  private final RiskScoringGridConfigManager riskScoringGridConfigManager;
  private final RiskFactorConfigsManager riskFactorConfigsManager;
  private final RiskScoringGridConfigValidator gridConfigValidator;
  private final RiskContributorConfigsValidator contributorConfigsValidator;

  @Override
  public void getRiskScoringGridConfig(
      GetRiskScoringGridConfigRequest request,
      StreamObserver<GetRiskScoringGridConfigResponse> responseObserver) {
    try {
      gridConfigValidator.validateGetRiskScoringGridConfigRequest(
          RequestContext.CURRENT.get(), request);
      GetRiskScoringGridConfigResponse response = buildGetRiskScoringGridConfigResponse(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Get risk scoring grid config failed with the request: {} and context: {}",
          request,
          RequestContext.CURRENT.get(),
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateRiskScoringGridConfig(
      UpdateRiskScoringGridConfigRequest request,
      StreamObserver<UpdateRiskScoringGridConfigResponse> responseObserver) {
    try {
      gridConfigValidator.validateUpdateRiskScoringGridConfigRequest(
          RequestContext.CURRENT.get(), request);
      UpdateRiskScoringGridConfigResponse response =
          buildUpdateRiskScoringGridConfigResponse(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Update risk scoring grid config failed with the request: {} and context: {}",
          request,
          RequestContext.CURRENT.get(),
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void resetRiskScoringGridConfig(
      ResetRiskScoringGridConfigRequest request,
      StreamObserver<ResetRiskScoringGridConfigResponse> responseObserver) {
    try {
      gridConfigValidator.validateResetRiskScoringGridConfigRequest(
          RequestContext.CURRENT.get(), request);
      ResetRiskScoringGridConfigResponse response =
          buildResetRiskScoringGridConfigResponse(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Reset risk scoring grid config failed with the request: {} and context: {}",
          request,
          RequestContext.CURRENT.get(),
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getRiskContributorConfigs(
      GetRiskContributorConfigsRequest request,
      StreamObserver<GetRiskContributorConfigsResponse> responseObserver) {
    try {
      contributorConfigsValidator.validateGetRiskContributorConfigsRequest(
          RequestContext.CURRENT.get(), request);
      GetRiskContributorConfigsResponse response = buildGetRiskContributorConfigsResponse(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Get risk Contributor configs failed with the request: {} and context: {}",
          request,
          RequestContext.CURRENT.get(),
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateRiskContributorConfigs(
      UpdateRiskContributorConfigsRequest request,
      StreamObserver<UpdateRiskContributorConfigsResponse> responseObserver) {
    try {
      contributorConfigsValidator.validateUpdateRiskContributorConfigsRequest(
          RequestContext.CURRENT.get(), request);
      UpdateRiskContributorConfigsResponse response =
          buildUpdateRiskContributorConfigsResponse(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Update risk Contributor configs failed with the request: {} and context: {}",
          request,
          RequestContext.CURRENT.get(),
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void resetRiskContributorConfigs(
      ResetRiskContributorConfigsRequest request,
      StreamObserver<ResetRiskContributorConfigsResponse> responseObserver) {
    try {
      contributorConfigsValidator.validateResetRiskContributorConfigsRequest(
          RequestContext.CURRENT.get(), request);
      responseObserver.onNext(
          ResetRiskContributorConfigsResponse.newBuilder()
              .setRiskConfigs(
                  riskFactorConfigsManager.resetRiskContributorConfigs(
                      RequestContext.CURRENT.get(),
                      request.getRiskFactorCategoriesList(),
                      request.getRiskConfigScope()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Reset risk Contributor configs failed with the request: {} and context: {}",
          request,
          RequestContext.CURRENT.get(),
          e);
      responseObserver.onError(e);
    }
  }

  private GetRiskScoringGridConfigResponse buildGetRiskScoringGridConfigResponse(
      GetRiskScoringGridConfigRequest request) {
    return GetRiskScoringGridConfigResponse.newBuilder()
        .setRiskLevelConfigValues(defaultRiskLevelConfigValues)
        .setRiskScoringGridConfig(
            riskScoringGridConfigManager.getRiskScoringGridConfig(
                RequestContext.CURRENT.get(), request.getRiskConfigScope()))
        .build();
  }

  private UpdateRiskScoringGridConfigResponse buildUpdateRiskScoringGridConfigResponse(
      UpdateRiskScoringGridConfigRequest request) {
    return UpdateRiskScoringGridConfigResponse.newBuilder()
        .setRiskLevelConfigValues(defaultRiskLevelConfigValues)
        .setRiskScoringGridConfig(
            riskScoringGridConfigManager.updateRiskScoringGridConfig(
                RequestContext.CURRENT.get(),
                request.getRiskConfigScope(),
                request.getRiskScoringGridCellsList()))
        .build();
  }

  private ResetRiskScoringGridConfigResponse buildResetRiskScoringGridConfigResponse(
      ResetRiskScoringGridConfigRequest request) {
    return ResetRiskScoringGridConfigResponse.newBuilder()
        .setRiskLevelConfigValues(defaultRiskLevelConfigValues)
        .setRiskScoringGridConfig(
            riskScoringGridConfigManager.resetRiskScoringGridConfig(
                RequestContext.CURRENT.get(), request.getRiskConfigScope()))
        .build();
  }

  private GetRiskContributorConfigsResponse buildGetRiskContributorConfigsResponse(
      GetRiskContributorConfigsRequest request) {
    return GetRiskContributorConfigsResponse.newBuilder()
        .setRiskConfigs(
            riskFactorConfigsManager.getRiskContributorConfigs(
                RequestContext.CURRENT.get(), request.getRiskConfigScope()))
        .build();
  }

  private UpdateRiskContributorConfigsResponse buildUpdateRiskContributorConfigsResponse(
      UpdateRiskContributorConfigsRequest request) {
    return UpdateRiskContributorConfigsResponse.newBuilder()
        .setRiskConfigs(
            riskFactorConfigsManager.updateRiskContributorConfigs(
                RequestContext.CURRENT.get(),
                request.getRiskFactorConfigUpdateDetailsList(),
                request.getRiskConfigScope()))
        .build();
  }
}
