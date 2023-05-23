package ai.traceable.anomalyscoring.config.service;

import ai.traceable.anomalyscoring.config.service.confidencelevel.ConfidenceScoringConfigManager;
import ai.traceable.anomalyscoring.config.service.impactlevel.ImpactScoringConfigManager;
import ai.traceable.anomalyscoring.config.service.v1.AnomalyScoringConfig;
import ai.traceable.anomalyscoring.config.service.v1.AnomalyScoringConfigServiceGrpc.AnomalyScoringConfigServiceImplBase;
import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoringConfig;
import ai.traceable.anomalyscoring.config.service.v1.GetAnomalyScoringConfigRequest;
import ai.traceable.anomalyscoring.config.service.v1.GetAnomalyScoringConfigResponse;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoringConfig;
import ai.traceable.anomalyscoring.config.service.v1.UpdateConfidenceScoringConfigRequest;
import ai.traceable.anomalyscoring.config.service.v1.UpdateConfidenceScoringConfigResponse;
import ai.traceable.anomalyscoring.config.service.v1.UpdateImpactScoringConfigRequest;
import ai.traceable.anomalyscoring.config.service.v1.UpdateImpactScoringConfigResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class AnomalyScoringConfigServiceImpl extends AnomalyScoringConfigServiceImplBase {

  private final AnomalyScoringConfigRequestValidator requestValidator;
  private final ImpactScoringConfigManager impactScoringConfigManager;
  private final ConfidenceScoringConfigManager confidenceScoringConfigManager;

  @Inject
  AnomalyScoringConfigServiceImpl(
      AnomalyScoringConfigRequestValidator requestValidator,
      ImpactScoringConfigManager impactScoringConfigManager,
      ConfidenceScoringConfigManager confidenceScoringConfigManager) {
    this.requestValidator = requestValidator;
    this.impactScoringConfigManager = impactScoringConfigManager;
    this.confidenceScoringConfigManager = confidenceScoringConfigManager;
  }

  @Override
  public void getAnomalyScoringConfig(
      GetAnomalyScoringConfigRequest request,
      StreamObserver<GetAnomalyScoringConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      ImpactScoringConfig impactScoringConfig =
          impactScoringConfigManager.getImpactScoringConfig(requestContext);
      ConfidenceScoringConfig confidenceScoringConfig =
          confidenceScoringConfigManager.getConfidenceScoringConfig(requestContext);

      AnomalyScoringConfig anomalyScoringConfig =
          AnomalyScoringConfig.newBuilder()
              .setImpactScoringConfig(impactScoringConfig)
              .setConfidenceScoringConfig(confidenceScoringConfig)
              .build();

      ImpactScoringConfig defaultImpactScoringConfig =
          impactScoringConfigManager.getDefaultImpactScoringConfig();
      ConfidenceScoringConfig defaultConfidenceScoringConfig =
          confidenceScoringConfigManager.getDefaultConfidenceScoringConfig();

      AnomalyScoringConfig defaultAnomalyScoringConfig =
          AnomalyScoringConfig.newBuilder()
              .setImpactScoringConfig(defaultImpactScoringConfig)
              .setConfidenceScoringConfig(defaultConfidenceScoringConfig)
              .build();

      responseObserver.onNext(
          GetAnomalyScoringConfigResponse.newBuilder()
              .setAnomalyScoringConfig(anomalyScoringConfig)
              .setDefaultAnomalyScoringConfig(defaultAnomalyScoringConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to get anomaly score config for request: {} and requestContext: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateImpactScoringConfig(
      UpdateImpactScoringConfigRequest request,
      StreamObserver<UpdateImpactScoringConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      ImpactScoringConfig impactScoringConfig =
          impactScoringConfigManager.upsertImpactScoringConfig(
              requestContext, request.getImpactScoringConfig());
      ImpactScoringConfig defaultImpactScoringConfig =
          impactScoringConfigManager.getDefaultImpactScoringConfig();

      responseObserver.onNext(
          UpdateImpactScoringConfigResponse.newBuilder()
              .setImpactScoringConfig(impactScoringConfig)
              .setDefaultImpactScoringConfig(defaultImpactScoringConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to update impact scoring config for request: {} and requestContext: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateConfidenceScoringConfig(
      UpdateConfidenceScoringConfigRequest request,
      StreamObserver<UpdateConfidenceScoringConfigResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      requestValidator.validateOrThrow(requestContext, request);
      ConfidenceScoringConfig confidenceScoringConfig =
          confidenceScoringConfigManager.upsertConfidenceScoringConfig(
              requestContext, request.getConfidenceScoringConfig());
      ConfidenceScoringConfig defaultConfidenceScoringConfig =
          confidenceScoringConfigManager.getDefaultConfidenceScoringConfig();

      responseObserver.onNext(
          UpdateConfidenceScoringConfigResponse.newBuilder()
              .setConfidenceScoringConfig(confidenceScoringConfig)
              .setDefaultConfidenceScoringConfig(defaultConfidenceScoringConfig)
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Unable to update confidence scoring config for request: {} and requestContext: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }
}
