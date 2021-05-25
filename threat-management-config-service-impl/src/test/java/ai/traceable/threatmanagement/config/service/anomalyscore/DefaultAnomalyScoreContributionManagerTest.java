package ai.traceable.threatmanagement.config.service.anomalyscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.ANOMALY_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.AnomalyScoreContribution;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
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
class DefaultAnomalyScoreContributionManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private static final int DEFAULT_ANOMALY_CONTRIBUTION_SCORE = 1;

  private static final AnomalyScoreContribution ANOMALY_SCORE_CONTRIBUTION_1 =
      AnomalyScoreContribution.newBuilder().setAnomalyScore(10).build();

  private static final Value ANOMALY_SCORE_CONTRIBUTION_CONFIG_1_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("anomalyScore", Value.newBuilder().setNumberValue(10).build()))
          .build();

  private static final AnomalyScoreContribution ANOMALY_SCORE_CONTRIBUTION_2 =
      AnomalyScoreContribution.newBuilder().setAnomalyScore(100).build();

  private static final Value ANOMALY_SCORE_CONTRIBUTION_CONFIG_2_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("anomalyScore", Value.newBuilder().setNumberValue(100).build()))
          .build();

  @Mock private ConfigServiceBlockingStub configServiceStub;

  @Mock private ThreatManagementConfigServiceConfig config;

  private AnomalyScoreContributionManager anomalyScoreContributionManager;

  @BeforeEach
  void setup() {
    configServiceStub = mock(ConfigServiceBlockingStub.class);
    this.anomalyScoreContributionManager =
        new DefaultAnomalyScoreContributionManager(
            configServiceStub, config, new AnomalyScoreContributionConverter());
  }

  @Test
  void shouldReturnDefaultAnomalyScoreContribution() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(ANOMALY_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();

    when(configServiceStub.getConfig(request)).thenReturn(GetConfigResponse.newBuilder().build());
    when(this.config.getDefaultAnomalyContributionScore())
        .thenReturn(DEFAULT_ANOMALY_CONTRIBUTION_SCORE);

    assertEquals(
        AnomalyScoreContribution.newBuilder()
            .setAnomalyScore(DEFAULT_ANOMALY_CONTRIBUTION_SCORE)
            .build(),
        anomalyScoreContributionManager.getAnomalyScoreContribution(REQUEST_CONTEXT));
  }

  @Test
  void shouldReturnAnomalyScoreContributionConfig() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(ANOMALY_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();
    when(configServiceStub.getConfig(request))
        .thenReturn(
            GetConfigResponse.newBuilder()
                .setConfig(ANOMALY_SCORE_CONTRIBUTION_CONFIG_1_VALUE)
                .build());

    assertEquals(
        ANOMALY_SCORE_CONTRIBUTION_1,
        anomalyScoreContributionManager.getAnomalyScoreContribution(REQUEST_CONTEXT));
  }

  @Test
  void shouldUpsertExistingAnomalyScoreContributionConfig() {
    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(ANOMALY_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .setConfig(ANOMALY_SCORE_CONTRIBUTION_CONFIG_2_VALUE)
            .setContext(TENANT_ID)
            .build();

    when(configServiceStub.upsertConfig(request))
        .thenReturn(
            UpsertConfigResponse.newBuilder()
                .setConfig(ANOMALY_SCORE_CONTRIBUTION_CONFIG_2_VALUE)
                .build());

    assertEquals(
        ANOMALY_SCORE_CONTRIBUTION_2,
        anomalyScoreContributionManager.upsertAnomalyScoreContribution(
            REQUEST_CONTEXT, ANOMALY_SCORE_CONTRIBUTION_2));
  }
}
