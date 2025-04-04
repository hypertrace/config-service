package ai.traceable.threatmanagement.config.service.threatscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.EnvironmentScope;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
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
class DefaultThreatScoreManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private static final int DEFAULT_UPPER_BOUND_LOW_THREAT_SCORE = 10;
  private static final int DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE = 20;
  private static final int DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE = 50;

  private static final ThreatScoreBound THREAT_SCORE_BOUND_1 =
      ThreatScoreBound.newBuilder()
          .setLowScoreUpperBound(30)
          .setMediumScoreUpperBound(50)
          .setHighScoreUpperBound(100)
          .build();

  private static final Value THREAT_BOUND_CONFIG_1_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("lowScoreUpperBound", Value.newBuilder().setNumberValue(30).build())
                  .putFields("mediumScoreUpperBound", Value.newBuilder().setNumberValue(50).build())
                  .putFields("highScoreUpperBound", Value.newBuilder().setNumberValue(100).build()))
          .build();

  private static final ThreatScoreBound THREAT_SCORE_BOUND_2 =
      ThreatScoreBound.newBuilder()
          .setLowScoreUpperBound(100)
          .setMediumScoreUpperBound(200)
          .setHighScoreUpperBound(300)
          .setScope(
              ScopeConfig.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder().setEnvironmentId("environment-id").build())
                  .build())
          .build();

  private static final Value THREAT_BOUND_CONFIG_2_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("lowScoreUpperBound", Value.newBuilder().setNumberValue(100).build())
                  .putFields(
                      "mediumScoreUpperBound", Value.newBuilder().setNumberValue(200).build())
                  .putFields("highScoreUpperBound", Value.newBuilder().setNumberValue(300).build())
                  .putFields(
                      "scope",
                      Value.newBuilder()
                          .setStructValue(
                              Struct.newBuilder()
                                  .putFields(
                                      "environmentScope",
                                      Value.newBuilder()
                                          .setStructValue(
                                              Struct.newBuilder()
                                                  .putFields(
                                                      "environmentId",
                                                      Value.newBuilder()
                                                          .setStringValue("environment-id")
                                                          .build())
                                                  .build())
                                          .build())
                                  .build())
                          .build()))
          .build();

  @Mock(answer = Answers.RETURNS_SELF)
  private ConfigServiceBlockingStub configServiceStub;

  @Mock private ThreatManagementConfigServiceConfig config;
  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  private ThreatScoreManager threatScoreManager;

  @BeforeEach
  void setup() {
    this.threatScoreManager =
        new DefaultThreatScoreManager(
            configServiceStub, configChangeEventGenerator, config, new ThreatScoreBoundConverter());
  }

  @Test
  void shouldReturnDefaultThreatScoreBounds() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();

    when(configServiceStub.getConfig(request)).thenReturn(GetConfigResponse.newBuilder().build());
    when(this.config.getDefaultThreatUpperBoundLowScore())
        .thenReturn(DEFAULT_UPPER_BOUND_LOW_THREAT_SCORE);
    when(this.config.getDefaultThreatUpperBoundMediumScore())
        .thenReturn(DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE);
    when(this.config.getDefaultThreatUpperBoundHighScore())
        .thenReturn(DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE);

    assertEquals(
        ThreatScoreBound.newBuilder()
            .setLowScoreUpperBound(DEFAULT_UPPER_BOUND_LOW_THREAT_SCORE)
            .setMediumScoreUpperBound(DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE)
            .setHighScoreUpperBound(DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE)
            .build(),
        threatScoreManager.getThreatScoreBound(REQUEST_CONTEXT, ScopeConfig.newBuilder().build()));
  }

  @Test
  void shouldReturnThreatScoreBoundConfig() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();
    when(configServiceStub.getConfig(request))
        .thenReturn(GetConfigResponse.newBuilder().setConfig(THREAT_BOUND_CONFIG_1_VALUE).build());

    assertEquals(
        THREAT_SCORE_BOUND_1,
        threatScoreManager.getThreatScoreBound(REQUEST_CONTEXT, ScopeConfig.newBuilder().build()));
  }

  @Test
  void shouldUpsertExistingThreatScoreBoundConfig() {
    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME)
            .setConfig(THREAT_BOUND_CONFIG_2_VALUE)
            .setContext("environment-id")
            .build();

    when(configServiceStub.upsertConfig(request))
        .thenReturn(
            UpsertConfigResponse.newBuilder().setConfig(THREAT_BOUND_CONFIG_2_VALUE).build());

    assertEquals(
        THREAT_SCORE_BOUND_2,
        threatScoreManager.upsertThreatScoreBound(REQUEST_CONTEXT, THREAT_SCORE_BOUND_2));
  }
}
