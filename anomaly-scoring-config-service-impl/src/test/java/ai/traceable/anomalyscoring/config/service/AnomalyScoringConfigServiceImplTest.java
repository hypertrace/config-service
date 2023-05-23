package ai.traceable.anomalyscoring.config.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomalyscoring.config.service.confidencelevel.ConfidenceScoringConfigManager;
import ai.traceable.anomalyscoring.config.service.impactlevel.ImpactScoringConfigManager;
import ai.traceable.anomalyscoring.config.service.v1.AnomalyScoringConfig;
import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoringConfig;
import ai.traceable.anomalyscoring.config.service.v1.GetAnomalyScoringConfigRequest;
import ai.traceable.anomalyscoring.config.service.v1.GetAnomalyScoringConfigResponse;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoringConfig;
import ai.traceable.anomalyscoring.config.service.v1.UpdateConfidenceScoringConfigRequest;
import ai.traceable.anomalyscoring.config.service.v1.UpdateConfidenceScoringConfigResponse;
import ai.traceable.anomalyscoring.config.service.v1.UpdateImpactScoringConfigRequest;
import ai.traceable.anomalyscoring.config.service.v1.UpdateImpactScoringConfigResponse;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class AnomalyScoringConfigServiceImplTest {
  private static final String TENANT_ID = "tenant-1";
  private static final int DEFAULT_IMPACT_SCORE_LEVEL_MEDIUM = 70;
  private static final int DEFAULT_IMPACT_SCORE_LEVEL_HIGH = 100;

  private static final int DEFAULT_CONFIDENCE_SCORE_LEVEL_MEDIUM = 90;
  private static final int DEFAULT_CONFIDENCE_SCORE_LEVEL_HIGH = 100;

  private static final ImpactScoringConfig DEFAULT_IMPACT_SCORING_CONFIG =
      ImpactScoringConfig.newBuilder()
          .setImpactScoreLevelConfig(
              ImpactScoreLevelConfig.newBuilder()
                  .setMediumLevelMinScore(DEFAULT_IMPACT_SCORE_LEVEL_MEDIUM)
                  .setHighLevelMinScore(DEFAULT_IMPACT_SCORE_LEVEL_HIGH)
                  .build())
          .build();
  private static final ImpactScoringConfig IMPACT_SCORING_CONFIG_1 =
      ImpactScoringConfig.newBuilder()
          .setImpactScoreLevelConfig(
              ImpactScoreLevelConfig.newBuilder()
                  .setMediumLevelMinScore(20)
                  .setHighLevelMinScore(30)
                  .build())
          .build();
  private static final ImpactScoringConfig IMPACT_SCORING_CONFIG_2 =
      ImpactScoringConfig.newBuilder()
          .setImpactScoreLevelConfig(
              ImpactScoreLevelConfig.newBuilder()
                  .setMediumLevelMinScore(200)
                  .setHighLevelMinScore(300)
                  .build())
          .build();

  private static final ConfidenceScoringConfig DEFAULT_CONFIDENCE_SCORING_CONFIG =
      ConfidenceScoringConfig.newBuilder()
          .setConfidenceScoreLevelConfig(
              ConfidenceScoreLevelConfig.newBuilder()
                  .setMediumLevelMinScore(DEFAULT_CONFIDENCE_SCORE_LEVEL_MEDIUM)
                  .setHighLevelMinScore(DEFAULT_CONFIDENCE_SCORE_LEVEL_HIGH)
                  .build())
          .build();
  private static final ConfidenceScoringConfig CONFIDENCE_SCORING_CONFIG_1 =
      ConfidenceScoringConfig.newBuilder()
          .setConfidenceScoreLevelConfig(
              ConfidenceScoreLevelConfig.newBuilder()
                  .setMediumLevelMinScore(20)
                  .setHighLevelMinScore(30)
                  .build())
          .build();
  private static final ConfidenceScoringConfig CONFIDENCE_SCORING_CONFIG_2 =
      ConfidenceScoringConfig.newBuilder()
          .setConfidenceScoreLevelConfig(
              ConfidenceScoreLevelConfig.newBuilder()
                  .setMediumLevelMinScore(200)
                  .setHighLevelMinScore(300)
                  .build())
          .build();

  @Mock private AnomalyScoringConfigRequestValidator requestValidator;
  @Mock private ImpactScoringConfigManager impactScoringConfigManager;
  @Mock private ConfidenceScoringConfigManager confidenceScoringConfigManager;

  private AnomalyScoringConfigServiceImpl AnomalyScoringConfigService;

  @BeforeEach
  void setup() {
    this.requestValidator = mock(AnomalyScoringConfigRequestValidator.class);
    doNothing()
        .when(requestValidator)
        .validateOrThrow(any(RequestContext.class), any(GetAnomalyScoringConfigRequest.class));
    doNothing()
        .when(requestValidator)
        .validateOrThrow(
            any(RequestContext.class), any(UpdateConfidenceScoringConfigRequest.class));
    doNothing()
        .when(requestValidator)
        .validateOrThrow(any(RequestContext.class), any(UpdateImpactScoringConfigRequest.class));

    this.impactScoringConfigManager = mock(ImpactScoringConfigManager.class);
    when(this.impactScoringConfigManager.getDefaultImpactScoringConfig())
        .thenReturn(DEFAULT_IMPACT_SCORING_CONFIG);

    this.confidenceScoringConfigManager = mock(ConfidenceScoringConfigManager.class);
    when(this.confidenceScoringConfigManager.getDefaultConfidenceScoringConfig())
        .thenReturn(DEFAULT_CONFIDENCE_SCORING_CONFIG);

    this.AnomalyScoringConfigService =
        new AnomalyScoringConfigServiceImpl(
            requestValidator, impactScoringConfigManager, confidenceScoringConfigManager);
  }

  @Nested
  class ImpactScoreLevelConfigs {
    @Test
    void getImpactScoreLevelConfig() {
      when(impactScoringConfigManager.getImpactScoringConfig(any(RequestContext.class)))
          .thenReturn(IMPACT_SCORING_CONFIG_1);

      when(confidenceScoringConfigManager.getConfidenceScoringConfig(any(RequestContext.class)))
          .thenReturn(CONFIDENCE_SCORING_CONFIG_1);

      StreamObserver<GetAnomalyScoringConfigResponse> responseObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              AnomalyScoringConfigService.getAnomalyScoringConfig(
                  GetAnomalyScoringConfigRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetAnomalyScoringConfigResponse.newBuilder()
                  .setAnomalyScoringConfig(
                      AnomalyScoringConfig.newBuilder()
                          .setImpactScoringConfig(IMPACT_SCORING_CONFIG_1)
                          .setConfidenceScoringConfig(CONFIDENCE_SCORING_CONFIG_1)
                          .build())
                  .setDefaultAnomalyScoringConfig(
                      AnomalyScoringConfig.newBuilder()
                          .setImpactScoringConfig(DEFAULT_IMPACT_SCORING_CONFIG)
                          .setConfidenceScoringConfig(DEFAULT_CONFIDENCE_SCORING_CONFIG)
                          .build())
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void updateSecurityEventScoreContribution() {
      when(impactScoringConfigManager.upsertImpactScoringConfig(
              any(RequestContext.class), eq(IMPACT_SCORING_CONFIG_1)))
          .thenReturn(IMPACT_SCORING_CONFIG_2);

      StreamObserver<UpdateImpactScoringConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              AnomalyScoringConfigService.updateImpactScoringConfig(
                  UpdateImpactScoringConfigRequest.newBuilder()
                      .setImpactScoringConfig(IMPACT_SCORING_CONFIG_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              UpdateImpactScoringConfigResponse.newBuilder()
                  .setImpactScoringConfig(IMPACT_SCORING_CONFIG_2)
                  .setDefaultImpactScoringConfig(DEFAULT_IMPACT_SCORING_CONFIG)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void skipUpdateSecurityEventScoreContribution() {
      when(impactScoringConfigManager.upsertImpactScoringConfig(
              any(RequestContext.class), eq(IMPACT_SCORING_CONFIG_1)))
          .thenThrow(IllegalArgumentException.class);

      StreamObserver<UpdateImpactScoringConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              AnomalyScoringConfigService.updateImpactScoringConfig(
                  UpdateImpactScoringConfigRequest.newBuilder()
                      .setImpactScoringConfig(IMPACT_SCORING_CONFIG_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.UNKNOWN));
    }
  }

  @Nested
  class ConfidenceScoreLevelConfigs {
    @Test
    void getConfidenceScoreLevelConfig() {
      when(impactScoringConfigManager.getImpactScoringConfig(any(RequestContext.class)))
          .thenReturn(IMPACT_SCORING_CONFIG_1);

      when(confidenceScoringConfigManager.getConfidenceScoringConfig(any(RequestContext.class)))
          .thenReturn(CONFIDENCE_SCORING_CONFIG_1);

      StreamObserver<GetAnomalyScoringConfigResponse> responseObserver = mock(StreamObserver.class);

      Runnable runnable =
          () ->
              AnomalyScoringConfigService.getAnomalyScoringConfig(
                  GetAnomalyScoringConfigRequest.getDefaultInstance(), responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              GetAnomalyScoringConfigResponse.newBuilder()
                  .setAnomalyScoringConfig(
                      AnomalyScoringConfig.newBuilder()
                          .setImpactScoringConfig(IMPACT_SCORING_CONFIG_1)
                          .setConfidenceScoringConfig(CONFIDENCE_SCORING_CONFIG_1)
                          .build())
                  .setDefaultAnomalyScoringConfig(
                      AnomalyScoringConfig.newBuilder()
                          .setImpactScoringConfig(DEFAULT_IMPACT_SCORING_CONFIG)
                          .setConfidenceScoringConfig(DEFAULT_CONFIDENCE_SCORING_CONFIG)
                          .build())
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void updateSecurityEventScoreContribution() {
      when(confidenceScoringConfigManager.upsertConfidenceScoringConfig(
              any(RequestContext.class), eq(CONFIDENCE_SCORING_CONFIG_1)))
          .thenReturn(CONFIDENCE_SCORING_CONFIG_2);

      StreamObserver<UpdateConfidenceScoringConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              AnomalyScoringConfigService.updateConfidenceScoringConfig(
                  UpdateConfidenceScoringConfigRequest.newBuilder()
                      .setConfidenceScoringConfig(CONFIDENCE_SCORING_CONFIG_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onNext(
              UpdateConfidenceScoringConfigResponse.newBuilder()
                  .setConfidenceScoringConfig(CONFIDENCE_SCORING_CONFIG_2)
                  .setDefaultConfidenceScoringConfig(DEFAULT_CONFIDENCE_SCORING_CONFIG)
                  .build());
      verify(responseObserver, times(1)).onCompleted();
    }

    @Test
    void skipUpdateSecurityEventScoreContribution() {
      when(confidenceScoringConfigManager.upsertConfidenceScoringConfig(
              any(RequestContext.class), eq(CONFIDENCE_SCORING_CONFIG_1)))
          .thenThrow(IllegalArgumentException.class);

      StreamObserver<UpdateConfidenceScoringConfigResponse> responseObserver =
          mock(StreamObserver.class);

      Runnable runnable =
          () ->
              AnomalyScoringConfigService.updateConfidenceScoringConfig(
                  UpdateConfidenceScoringConfigRequest.newBuilder()
                      .setConfidenceScoringConfig(CONFIDENCE_SCORING_CONFIG_1)
                      .build(),
                  responseObserver);
      GrpcClientRequestContextUtil.executeInTenantContext(TENANT_ID, runnable);

      verify(responseObserver, times(1))
          .onError(argThat(err -> Status.fromThrowable(err).getCode() == Code.UNKNOWN));
    }
  }
}
