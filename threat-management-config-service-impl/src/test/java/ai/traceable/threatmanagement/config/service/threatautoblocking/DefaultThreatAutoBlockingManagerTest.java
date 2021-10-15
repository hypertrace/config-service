package ai.traceable.threatmanagement.config.service.threatautoblocking;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig.ExpirationDetails;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
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
class DefaultThreatAutoBlockingManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private static final ThreatAutoBlockingActionConfig THREAT_AUTO_BLOCKING_ACTION_CONFIG_1 =
      ThreatAutoBlockingActionConfig.newBuilder()
          .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
          .build();

  private static final Value THREAT_AUTO_BLOCKING_ACTION_CONFIG_1_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields(
                      "actionType",
                      Value.newBuilder()
                          .setStringValue(
                              ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK
                                  .name())
                          .build())
                  .build())
          .build();

  private static final ThreatAutoBlockingActionConfig THREAT_AUTO_BLOCKING_ACTION_CONFIG_2 =
      ThreatAutoBlockingActionConfig.newBuilder()
          .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
          .setExpirationDetails(
              ExpirationDetails.newBuilder().setDuration("PT1H2M34S").setTimestampMillis(3754000))
          .build();

  private static final Value THREAT_AUTO_BLOCKING_ACTION_CONFIG_2_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields(
                      "actionType",
                      Value.newBuilder()
                          .setStringValue(
                              ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK
                                  .name())
                          .build())
                  .putFields(
                      "expirationDetails",
                      Value.newBuilder()
                          .setStructValue(
                              Struct.newBuilder()
                                  .putFields(
                                      "duration",
                                      Value.newBuilder().setStringValue("PT1H2M34S").build())
                                  .putFields(
                                      "timestampMillis",
                                      Value.newBuilder().setStringValue("3754000").build())
                                  .build())
                          .build()))
          .build();

  @Mock private ConfigServiceBlockingStub configServiceStub;
  @Mock private ThreatAutoBlockingActionConfigConverter configConverter;
  private ThreatAutoBlockingManager threatAutoBlockingManager;

  @BeforeEach
  void setup() {
    configServiceStub = mock(ConfigServiceBlockingStub.class);
    this.configConverter = mock(ThreatAutoBlockingActionConfigConverter.class);
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    this.threatAutoBlockingManager =
        new DefaultThreatAutoBlockingManager(
            configServiceStub,
            configChangeEventGenerator,
            configConverter,
            new ThreatAutoBlockingConverter());
  }

  @Test
  void shouldReturnDefaultThreatAutoBlockingAction() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();

    when(configServiceStub.getConfig(request)).thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(
        ThreatAutoBlockingActionConfig.newBuilder()
            .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION)
            .build(),
        threatAutoBlockingManager.getThreatAutoBlockingAction(REQUEST_CONTEXT));
  }

  @Test
  void shouldReturnThreatAutoBlockingActionConfig() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();
    when(configServiceStub.getConfig(request))
        .thenReturn(
            GetConfigResponse.newBuilder()
                .setConfig(THREAT_AUTO_BLOCKING_ACTION_CONFIG_1_VALUE)
                .build());

    assertEquals(
        THREAT_AUTO_BLOCKING_ACTION_CONFIG_1,
        threatAutoBlockingManager.getThreatAutoBlockingAction(REQUEST_CONTEXT));
  }

  @Test
  void shouldReturnThreatAutoBlockingActionConfig_expiration() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();
    when(configServiceStub.getConfig(request))
        .thenReturn(
            GetConfigResponse.newBuilder()
                .setConfig(THREAT_AUTO_BLOCKING_ACTION_CONFIG_2_VALUE)
                .build());

    assertEquals(
        THREAT_AUTO_BLOCKING_ACTION_CONFIG_2,
        threatAutoBlockingManager.getThreatAutoBlockingAction(REQUEST_CONTEXT));
  }

  @Test
  void shouldUpsertExistingThreatAutoBlockingActionConfig() {
    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME)
            .setConfig(THREAT_AUTO_BLOCKING_ACTION_CONFIG_2_VALUE)
            .setContext(TENANT_ID)
            .build();

    when(configServiceStub.upsertConfig(request))
        .thenReturn(
            UpsertConfigResponse.newBuilder()
                .setConfig(THREAT_AUTO_BLOCKING_ACTION_CONFIG_2_VALUE)
                .build());

    UpdateThreatAutoBlockingConfigRequest updateThreatAutoBlockingConfigRequest =
        UpdateThreatAutoBlockingConfigRequest.newBuilder()
            .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK)
            .setExpirationDetails(
                UpdateThreatAutoBlockingConfigRequest.ExpirationDetails.newBuilder()
                    .setDuration("PT1H2M34S"))
            .build();
    when(configConverter.convert(updateThreatAutoBlockingConfigRequest))
        .thenReturn(THREAT_AUTO_BLOCKING_ACTION_CONFIG_2);

    assertEquals(
        THREAT_AUTO_BLOCKING_ACTION_CONFIG_2,
        threatAutoBlockingManager.upsertThreatAutoBlockingAction(
            REQUEST_CONTEXT, updateThreatAutoBlockingConfigRequest));
  }
}
