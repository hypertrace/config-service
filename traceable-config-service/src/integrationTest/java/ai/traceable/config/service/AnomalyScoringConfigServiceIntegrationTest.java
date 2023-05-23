package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import ai.traceable.anomalyscoring.config.service.v1.AnomalyScoringConfig;
import ai.traceable.anomalyscoring.config.service.v1.AnomalyScoringConfigServiceGrpc;
import ai.traceable.anomalyscoring.config.service.v1.AnomalyScoringConfigServiceGrpc.AnomalyScoringConfigServiceBlockingStub;
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
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for AnomalyScoringConfigService */
public class AnomalyScoringConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {

  private static final int DEFAULT_IMPACT_SCORE_LEVEL_MEDIUM = 30;
  private static final int DEFAULT_IMPACT_SCORE_LEVEL_HIGH = 70;

  private static final int DEFAULT_CONFIDENCE_SCORE_LEVEL_MEDIUM = 30;
  private static final int DEFAULT_CONFIDENCE_SCORE_LEVEL_HIGH = 90;

  private static final ImpactScoringConfig DEFAULT_IMPACT_SCORING_CONFIG =
      ImpactScoringConfig.newBuilder()
          .setImpactScoreLevelConfig(
              ImpactScoreLevelConfig.newBuilder()
                  .setMediumLevelMinScore(DEFAULT_IMPACT_SCORE_LEVEL_MEDIUM)
                  .setHighLevelMinScore(DEFAULT_IMPACT_SCORE_LEVEL_HIGH)
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

  private static AnomalyScoringConfigServiceBlockingStub anomalyScoringConfigServiceStub;

  @BeforeAll
  static void init() {
    anomalyScoringConfigServiceStub =
        AnomalyScoringConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testAnomalyScoringConfig() {
    // Get default scoring config
    GetAnomalyScoringConfigResponse getAnomalyScoringConfigResponse =
        this.executeGetAnomalyScoringConfig();
    AnomalyScoringConfig defaultAnomalyScoringConfig =
        getAnomalyScoringConfigResponse.getDefaultAnomalyScoringConfig();
    AnomalyScoringConfig anomalyScoringConfig =
        getAnomalyScoringConfigResponse.getAnomalyScoringConfig();
    // Assert default scoring configs
    assertEquals(anomalyScoringConfig, defaultAnomalyScoringConfig);
    assertImpactConfig(anomalyScoringConfig.getImpactScoringConfig(), 30, 70);
    assertConfidenceConfig(anomalyScoringConfig.getConfidenceScoringConfig(), 30, 90);

    // Update impact scoring
    ImpactScoringConfig updateImpactScoringConfig =
        ImpactScoringConfig.newBuilder()
            .setImpactScoreLevelConfig(
                ImpactScoreLevelConfig.newBuilder()
                    .setMediumLevelMinScore(50)
                    .setHighLevelMinScore(80)
                    .build())
            .build();
    UpdateImpactScoringConfigResponse updateImpactScoringConfigResponse =
        executeUpdateImpactScoringConfig(updateImpactScoringConfig);
    ImpactScoringConfig defaultImpactScoringConfig =
        updateImpactScoringConfigResponse.getDefaultImpactScoringConfig();
    ImpactScoringConfig impactScoringConfig =
        updateImpactScoringConfigResponse.getImpactScoringConfig();
    assertEquals(DEFAULT_IMPACT_SCORING_CONFIG, defaultImpactScoringConfig);
    assertImpactConfig(impactScoringConfig, 50, 80);

    // Update Confidence scoring
    ConfidenceScoringConfig updateConfidenceScoringConfig =
        ConfidenceScoringConfig.newBuilder()
            .setConfidenceScoreLevelConfig(
                ConfidenceScoreLevelConfig.newBuilder()
                    .setMediumLevelMinScore(40)
                    .setHighLevelMinScore(70)
                    .build())
            .build();
    UpdateConfidenceScoringConfigResponse updateConfidenceScoringConfigResponse =
        executeUpdateConfidenceScoringConfig(updateConfidenceScoringConfig);
    ConfidenceScoringConfig defaultConfidenceScoringConfig =
        updateConfidenceScoringConfigResponse.getDefaultConfidenceScoringConfig();
    ConfidenceScoringConfig confidenceScoringConfig =
        updateConfidenceScoringConfigResponse.getConfidenceScoringConfig();
    assertEquals(DEFAULT_CONFIDENCE_SCORING_CONFIG, defaultConfidenceScoringConfig);
    assertConfidenceConfig(confidenceScoringConfig, 40, 70);

    // Check for updated configs
    getAnomalyScoringConfigResponse = this.executeGetAnomalyScoringConfig();
    defaultAnomalyScoringConfig = getAnomalyScoringConfigResponse.getDefaultAnomalyScoringConfig();
    anomalyScoringConfig = getAnomalyScoringConfigResponse.getAnomalyScoringConfig();
    // Assert default scoring configs
    assertNotEquals(anomalyScoringConfig, defaultAnomalyScoringConfig);
    assertImpactConfig(anomalyScoringConfig.getImpactScoringConfig(), 50, 80);
    assertConfidenceConfig(anomalyScoringConfig.getConfidenceScoringConfig(), 40, 70);
  }

  private void assertImpactConfig(
      ImpactScoringConfig impactScoringConfig,
      long expectedMediumMinScore,
      long expectedHighMinScore) {
    assertEquals(
        expectedMediumMinScore,
        impactScoringConfig.getImpactScoreLevelConfig().getMediumLevelMinScore());
    assertEquals(
        expectedHighMinScore,
        impactScoringConfig.getImpactScoreLevelConfig().getHighLevelMinScore());
  }

  private void assertConfidenceConfig(
      ConfidenceScoringConfig confidenceScoringConfig,
      long expectedMediumMinScore,
      long expectedHighMinScore) {
    assertEquals(
        expectedMediumMinScore,
        confidenceScoringConfig.getConfidenceScoreLevelConfig().getMediumLevelMinScore());
    assertEquals(
        expectedHighMinScore,
        confidenceScoringConfig.getConfidenceScoreLevelConfig().getHighLevelMinScore());
  }

  private UpdateImpactScoringConfigResponse executeUpdateImpactScoringConfig(
      ImpactScoringConfig config) {
    UpdateImpactScoringConfigRequest request =
        UpdateImpactScoringConfigRequest.newBuilder().setImpactScoringConfig(config).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> anomalyScoringConfigServiceStub.updateImpactScoringConfig(request));
  }

  private UpdateConfidenceScoringConfigResponse executeUpdateConfidenceScoringConfig(
      ConfidenceScoringConfig config) {
    UpdateConfidenceScoringConfigRequest request =
        UpdateConfidenceScoringConfigRequest.newBuilder()
            .setConfidenceScoringConfig(config)
            .build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> anomalyScoringConfigServiceStub.updateConfidenceScoringConfig(request));
  }

  private GetAnomalyScoringConfigResponse executeGetAnomalyScoringConfig() {
    GetAnomalyScoringConfigRequest request = GetAnomalyScoringConfigRequest.newBuilder().build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> anomalyScoringConfigServiceStub.getAnomalyScoringConfig(request));
  }
}
