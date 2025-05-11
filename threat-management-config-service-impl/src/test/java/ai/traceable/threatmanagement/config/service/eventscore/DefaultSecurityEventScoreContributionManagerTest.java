package ai.traceable.threatmanagement.config.service.eventscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.EnvironmentScope;
import ai.traceable.threatmanagement.config.service.v1.EventConfidenceLevel;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
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
class DefaultSecurityEventScoreContributionManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_LOW_SCORE = 1;
  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE = 2;
  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE = 3;
  private static final int DEFAULT_SECURITY_EVENT_CONTRIBUTION_CRITICAL_SCORE = 10;
  private static final EventConfidenceLevel DEFAULT_MINIMUM_EVENT_CONFIDENCE_LEVEL =
      EventConfidenceLevel.EVENT_CONFIDENCE_LEVEL_LOW;

  private static final SecurityEventScoreContribution SECURITY_EVENT_SCORE_CONTRIBUTION_1 =
      SecurityEventScoreContribution.newBuilder()
          .setLowScore(10)
          .setMediumScore(20)
          .setHighScore(30)
          .setCriticalScore(100)
          .build();

  private static final Value SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_1_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("lowScore", Value.newBuilder().setNumberValue(10).build())
                  .putFields("mediumScore", Value.newBuilder().setNumberValue(20).build())
                  .putFields("highScore", Value.newBuilder().setNumberValue(30).build())
                  .putFields("criticalScore", Value.newBuilder().setNumberValue(100).build())
                  .build())
          .build();

  private static final SecurityEventScoreContribution SECURITY_EVENT_SCORE_CONTRIBUTION_2 =
      SecurityEventScoreContribution.newBuilder()
          .setLowScore(100)
          .setMediumScore(200)
          .setHighScore(300)
          .setCriticalScore(1000)
          .setScope(
              ScopeConfig.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder().setEnvironmentId("environment-id").build())
                  .build())
          .build();

  private static final Value SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_2_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("lowScore", Value.newBuilder().setNumberValue(100).build())
                  .putFields("mediumScore", Value.newBuilder().setNumberValue(200).build())
                  .putFields("highScore", Value.newBuilder().setNumberValue(300).build())
                  .putFields("criticalScore", Value.newBuilder().setNumberValue(1000).build())
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
                          .build())
                  .build())
          .build();

  @Mock(answer = Answers.RETURNS_SELF)
  private ConfigServiceBlockingStub configServiceStub;

  @Mock private ThreatManagementConfigServiceConfig config;
  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  private SecurityEventScoreContributionManager securityEventScoreContributionManager;

  @BeforeEach
  void setup() {
    this.securityEventScoreContributionManager =
        new DefaultSecurityEventScoreContributionManager(
            configServiceStub,
            configChangeEventGenerator,
            config,
            new SecurityEventScoreContributionConverter());
  }

  @Test
  void shouldReturnDefaultSecurityEventScoreContribution() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();

    when(configServiceStub.getConfig(request)).thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(this.config.getDefaultSecurityEventContributionLowScore())
        .thenReturn(DEFAULT_SECURITY_EVENT_CONTRIBUTION_LOW_SCORE);
    when(this.config.getDefaultSecurityEventContributionMediumScore())
        .thenReturn(DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE);
    when(this.config.getDefaultSecurityEventContributionHighScore())
        .thenReturn(DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE);
    when(this.config.getDefaultSecurityEventContributionCriticalScore())
        .thenReturn(DEFAULT_SECURITY_EVENT_CONTRIBUTION_CRITICAL_SCORE);
    when(this.config.getMinimumEventConfidenceLevel())
        .thenReturn(DEFAULT_MINIMUM_EVENT_CONFIDENCE_LEVEL);

    assertEquals(
        SecurityEventScoreContribution.newBuilder()
            .setLowScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_LOW_SCORE)
            .setMediumScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE)
            .setHighScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE)
            .setCriticalScore(DEFAULT_SECURITY_EVENT_CONTRIBUTION_CRITICAL_SCORE)
            .setMinimumEventConfidenceLevel(DEFAULT_MINIMUM_EVENT_CONFIDENCE_LEVEL)
            .build(),
        securityEventScoreContributionManager.getSecurityEventScoreContribution(
            REQUEST_CONTEXT, ScopeConfig.newBuilder().build()));
  }

  @Test
  void shouldReturnSecurityEventScoreContributionConfig() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();
    when(configServiceStub.getConfig(request))
        .thenReturn(
            GetConfigResponse.newBuilder()
                .setConfig(SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_1_VALUE)
                .build());

    assertEquals(
        SECURITY_EVENT_SCORE_CONTRIBUTION_1,
        securityEventScoreContributionManager.getSecurityEventScoreContribution(
            REQUEST_CONTEXT, ScopeConfig.newBuilder().build()));
  }

  @Test
  void shouldUpsertExistingSecurityEventScoreContributionConfig() {
    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .setConfig(SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_2_VALUE)
            .setContext("environment-id")
            .build();

    when(configServiceStub.upsertConfig(request))
        .thenReturn(
            UpsertConfigResponse.newBuilder()
                .setConfig(SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_2_VALUE)
                .build());

    assertEquals(
        SECURITY_EVENT_SCORE_CONTRIBUTION_2,
        securityEventScoreContributionManager.upsertSecurityEventScoreContribution(
            REQUEST_CONTEXT, SECURITY_EVENT_SCORE_CONTRIBUTION_2));
  }
}
