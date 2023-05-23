package ai.traceable.anomalyscoring.config.service.confidencelevel;

import static ai.traceable.anomalyscoring.config.service.constants.AnomalyScoringConfigConstants.ANOMALY_SCORING_CONFIG_NAMESPACE;
import static ai.traceable.anomalyscoring.config.service.constants.AnomalyScoringConfigConstants.CONFIDENCE_CONFIG_RESOURCE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomalyscoring.config.service.AnomalyScoringConfigServiceConfig;
import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoringConfig;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultConfidenceScoringConfigManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private static final int DEFAULT_CONFIDENCE_SCORE_LEVEL_MEDIUM = 30;
  private static final int DEFAULT_CONFIDENCE_SCORE_LEVEL_HIGH = 90;

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

  private static final Value CONFIDENCE_SCORING_CONFIG_1_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields(
                      "confidenceScoreLevelConfig",
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

  private static final Value CONFIDENCE_SCORING_CONFIG_2_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields(
                      "confidenceScoreLevelConfig",
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

  @Mock private ConfigServiceBlockingStub configServiceStub;

  @Mock private AnomalyScoringConfigServiceConfig config;

  private ConfidenceScoringConfigManager confidenceScoringConfigManager;

  @BeforeEach
  void setup() {
    configServiceStub = mock(ConfigServiceBlockingStub.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    this.confidenceScoringConfigManager =
        new DefaultConfidenceScoringConfigManager(
            configServiceStub,
            configChangeEventGenerator,
            config,
            new ConfidenceScoringConfigConverter());
  }

  @Test
  void shouldReturnDefaultAnomalyScoringConfig() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(ANOMALY_SCORING_CONFIG_NAMESPACE)
            .setResourceName(CONFIDENCE_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();

    when(configServiceStub.getConfig(request)).thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(this.config.getDefaultMediumConfidenceMinScore())
        .thenReturn(DEFAULT_CONFIDENCE_SCORE_LEVEL_MEDIUM);
    when(this.config.getDefaultHighConfidenceMinScore())
        .thenReturn(DEFAULT_CONFIDENCE_SCORE_LEVEL_HIGH);

    assertEquals(
        ConfidenceScoringConfig.newBuilder()
            .setConfidenceScoreLevelConfig(
                ConfidenceScoreLevelConfig.newBuilder()
                    .setMediumLevelMinScore(DEFAULT_CONFIDENCE_SCORE_LEVEL_MEDIUM)
                    .setHighLevelMinScore(DEFAULT_CONFIDENCE_SCORE_LEVEL_HIGH)
                    .build())
            .build(),
        confidenceScoringConfigManager.getConfidenceScoringConfig(REQUEST_CONTEXT));
  }

  @Test
  void shouldReturnAnomalyScoringConfig() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(ANOMALY_SCORING_CONFIG_NAMESPACE)
            .setResourceName(CONFIDENCE_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();
    when(configServiceStub.getConfig(request))
        .thenReturn(
            GetConfigResponse.newBuilder().setConfig(CONFIDENCE_SCORING_CONFIG_1_VALUE).build());

    assertEquals(
        CONFIDENCE_SCORING_CONFIG_1,
        confidenceScoringConfigManager.getConfidenceScoringConfig(REQUEST_CONTEXT));
  }

  @Test
  void shouldUpsertExistingAnomalyScoringConfig() {
    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(ANOMALY_SCORING_CONFIG_NAMESPACE)
            .setResourceName(CONFIDENCE_CONFIG_RESOURCE_NAME)
            .setConfig(CONFIDENCE_SCORING_CONFIG_2_VALUE)
            .setContext(TENANT_ID)
            .build();

    when(configServiceStub.upsertConfig(request))
        .thenReturn(
            UpsertConfigResponse.newBuilder().setConfig(CONFIDENCE_SCORING_CONFIG_2_VALUE).build());

    assertEquals(
        CONFIDENCE_SCORING_CONFIG_2,
        confidenceScoringConfigManager.upsertConfidenceScoringConfig(
            REQUEST_CONTEXT, CONFIDENCE_SCORING_CONFIG_2));
  }
}
