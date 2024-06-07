package ai.traceable.anomalyscoring.config.service.impactlevel;

import static ai.traceable.anomalyscoring.config.service.constants.AnomalyScoringConfigConstants.ANOMALY_SCORING_CONFIG_NAMESPACE;
import static ai.traceable.anomalyscoring.config.service.constants.AnomalyScoringConfigConstants.IMPACT_CONFIG_RESOURCE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.anomalyscoring.config.service.AnomalyScoringConfigServiceConfig;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoringConfig;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.Status;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultImpactScoringConfigManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private static final int DEFAULT_IMPACT_SCORE_LEVEL_MEDIUM = 30;
  private static final int DEFAULT_IMPACT_SCORE_LEVEL_HIGH = 70;

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

  private static final Value IMPACT_SCORING_CONFIG_1_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields(
                      "impactScoreLevelConfig",
                      Value.newBuilder()
                          .setStructValue(
                              Struct.newBuilder()
                                  .putFields(
                                      "mediumLevelMinScore",
                                      Value.newBuilder().setNumberValue(20).build())
                                  .putFields(
                                      "highLevelMinScore",
                                      Value.newBuilder().setNumberValue(30).build()))
                          .build()))
          .build();

  private static final Value IMPACT_SCORING_CONFIG_2_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields(
                      "impactScoreLevelConfig",
                      Value.newBuilder()
                          .setStructValue(
                              Struct.newBuilder()
                                  .putFields(
                                      "mediumLevelMinScore",
                                      Value.newBuilder().setNumberValue(200).build())
                                  .putFields(
                                      "highLevelMinScore",
                                      Value.newBuilder().setNumberValue(300).build()))
                          .build()))
          .build();

  @Mock(answer = Answers.RETURNS_SELF)
  private ConfigServiceBlockingStub configServiceStub;

  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  @Mock private AnomalyScoringConfigServiceConfig config;

  private ImpactScoringConfigManager impactScoringConfigManager;

  @BeforeEach
  void setup() {
    this.impactScoringConfigManager =
        new DefaultImpactScoringConfigManager(
            configServiceStub,
            configChangeEventGenerator,
            config,
            new ImpactScoringConfigConverter());
  }

  @Test
  void shouldReturnDefaultAnomalyScoringConfig() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(ANOMALY_SCORING_CONFIG_NAMESPACE)
            .setResourceName(IMPACT_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();

    when(configServiceStub.getConfig(request)).thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(this.config.getDefaultMediumImpactMinScore())
        .thenReturn(DEFAULT_IMPACT_SCORE_LEVEL_MEDIUM);
    when(this.config.getDefaultHighImpactMinScore()).thenReturn(DEFAULT_IMPACT_SCORE_LEVEL_HIGH);

    assertEquals(
        ImpactScoringConfig.newBuilder()
            .setImpactScoreLevelConfig(
                ImpactScoreLevelConfig.newBuilder()
                    .setMediumLevelMinScore(DEFAULT_IMPACT_SCORE_LEVEL_MEDIUM)
                    .setHighLevelMinScore(DEFAULT_IMPACT_SCORE_LEVEL_HIGH)
                    .build())
            .build(),
        impactScoringConfigManager.getImpactScoringConfig(REQUEST_CONTEXT));
  }

  @Test
  void shouldReturnAnomalyScoringConfig() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(ANOMALY_SCORING_CONFIG_NAMESPACE)
            .setResourceName(IMPACT_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();
    when(configServiceStub.getConfig(request))
        .thenReturn(
            GetConfigResponse.newBuilder().setConfig(IMPACT_SCORING_CONFIG_1_VALUE).build());

    assertEquals(
        IMPACT_SCORING_CONFIG_1,
        impactScoringConfigManager.getImpactScoringConfig(REQUEST_CONTEXT));
  }

  @Test
  void shouldUpsertExistingAnomalyScoringConfig() {
    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(ANOMALY_SCORING_CONFIG_NAMESPACE)
            .setResourceName(IMPACT_CONFIG_RESOURCE_NAME)
            .setConfig(IMPACT_SCORING_CONFIG_2_VALUE)
            .setContext(TENANT_ID)
            .build();

    when(configServiceStub.upsertConfig(request))
        .thenReturn(
            UpsertConfigResponse.newBuilder().setConfig(IMPACT_SCORING_CONFIG_2_VALUE).build());

    assertEquals(
        IMPACT_SCORING_CONFIG_2,
        impactScoringConfigManager.upsertImpactScoringConfig(
            REQUEST_CONTEXT, IMPACT_SCORING_CONFIG_2));
  }
}
