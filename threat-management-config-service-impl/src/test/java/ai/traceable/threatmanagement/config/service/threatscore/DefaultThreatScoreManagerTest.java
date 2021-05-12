package ai.traceable.threatmanagement.config.service.threatscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import com.google.protobuf.InvalidProtocolBufferException;
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
class DefaultThreatScoreManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private static final int DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE = 10;
  private static final int DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE = 20;

  private static final ThreatScoreBound THREAT_SCORE_BOUND_1 =
      ThreatScoreBound.newBuilder()
          .setMediumScoreUpperBound(50)
          .setHighScoreUpperBound(100)
          .build();

  private static final Value THREAT_BOUND_CONFIG_1_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields("mediumScoreUpperBound", Value.newBuilder().setNumberValue(50).build())
                  .putFields("highScoreUpperBound", Value.newBuilder().setNumberValue(100).build()))
          .build();

  private static final ThreatScoreBound THREAT_SCORE_BOUND_2 =
      ThreatScoreBound.newBuilder()
          .setMediumScoreUpperBound(200)
          .setHighScoreUpperBound(300)
          .build();

  private static final Value THREAT_BOUND_CONFIG_2_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields(
                      "mediumScoreUpperBound", Value.newBuilder().setNumberValue(200).build())
                  .putFields("highScoreUpperBound", Value.newBuilder().setNumberValue(300).build()))
          .build();

  @Mock private ConfigServiceBlockingStub configServiceStub;

  @Mock private ThreatManagementConfigServiceConfig config;

  private ThreatScoreManager threatScoreManager;

  @BeforeEach
  void setup() {
    configServiceStub = mock(ConfigServiceBlockingStub.class);
    this.threatScoreManager =
        new DefaultThreatScoreManager(configServiceStub, config, new ThreatScoreBoundConverter());
  }

  @Test
  void shouldReturnDefaultThreatScoreBounds() throws InvalidProtocolBufferException {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();

    when(configServiceStub.getConfig(request)).thenReturn(GetConfigResponse.newBuilder().build());
    when(this.config.getDefaultThreatUpperBoundMediumScore())
        .thenReturn(DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE);
    when(this.config.getDefaultThreatUpperBoundHighScore())
        .thenReturn(DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE);

    assertEquals(
        ThreatScoreBound.newBuilder()
            .setMediumScoreUpperBound(DEFAULT_UPPER_BOUND_MEDIUM_THREAT_SCORE)
            .setHighScoreUpperBound(DEFAULT_UPPER_BOUND_HIGH_THREAT_SCORE)
            .build(),
        threatScoreManager.getThreatScoreBound(REQUEST_CONTEXT));
  }

  @Test
  void shouldReturnThreatScoreBoundConfig() throws InvalidProtocolBufferException {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();
    when(configServiceStub.getConfig(request))
        .thenReturn(GetConfigResponse.newBuilder().setConfig(THREAT_BOUND_CONFIG_1_VALUE).build());

    assertEquals(THREAT_SCORE_BOUND_1, threatScoreManager.getThreatScoreBound(REQUEST_CONTEXT));
  }

  @Test
  void shouldUpsertExistingThreatScoreBoundConfig() throws InvalidProtocolBufferException {
    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME)
            .setConfig(THREAT_BOUND_CONFIG_2_VALUE)
            .setContext(TENANT_ID)
            .build();

    when(configServiceStub.upsertConfig(request))
        .thenReturn(
            UpsertConfigResponse.newBuilder().setConfig(THREAT_BOUND_CONFIG_2_VALUE).build());

    assertEquals(
        THREAT_SCORE_BOUND_2,
        threatScoreManager.upsertThreatScoreBound(REQUEST_CONTEXT, THREAT_SCORE_BOUND_2));
  }
}
