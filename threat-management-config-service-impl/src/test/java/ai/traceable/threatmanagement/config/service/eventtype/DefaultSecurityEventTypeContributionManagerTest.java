package ai.traceable.threatmanagement.config.service.eventtype;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.v1.EnvironmentScope;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution.SecurityEventTypeContributionKind;
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
class DefaultSecurityEventTypeContributionManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private static final SecurityEventTypeContribution SECURITY_EVENT_TYPE_CONTRIBUTION_1 =
      SecurityEventTypeContribution.newBuilder()
          .setSecurityEventTypeContributionKind(
              SecurityEventTypeContributionKind.SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_ALL)
          .build();

  private static final Value SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_1_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields(
                      "securityEventTypeContributionKind",
                      Value.newBuilder()
                          .setStringValue(
                              SecurityEventTypeContributionKind
                                  .SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_ALL
                                  .name())
                          .build())
                  .build())
          .build();

  private static final SecurityEventTypeContribution SECURITY_EVENT_TYPE_CONTRIBUTION_2 =
      SecurityEventTypeContribution.newBuilder()
          .setSecurityEventTypeContributionKind(
              SecurityEventTypeContributionKind
                  .SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_HIGH_RISK_APIS)
          .setScope(
              ScopeConfig.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder().setEnvironmentId("environment-id").build()))
          .build();

  private static final Value SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_2_VALUE =
      Value.newBuilder()
          .setStructValue(
              Struct.newBuilder()
                  .putFields(
                      "securityEventTypeContributionKind",
                      Value.newBuilder()
                          .setStringValue(
                              SecurityEventTypeContributionKind
                                  .SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_HIGH_RISK_APIS
                                  .name())
                          .build())
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

  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  private SecurityEventTypeContributionManager securityEventTypeContributionManager;

  @BeforeEach
  void setup() {
    this.securityEventTypeContributionManager =
        new DefaultSecurityEventTypeContributionManager(
            configServiceStub,
            new SecurityEventTypeContributionConverter(),
            configChangeEventGenerator);
  }

  @Test
  void shouldReturnDefaultSecurityEventTypeContribution() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();

    when(configServiceStub.getConfig(request)).thenReturn(GetConfigResponse.newBuilder().build());

    assertEquals(
        SecurityEventTypeContribution.newBuilder()
            .setSecurityEventTypeContributionKind(
                SecurityEventTypeContributionKind.SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_ALL)
            .build(),
        securityEventTypeContributionManager.getSecurityEventTypeContribution(
            REQUEST_CONTEXT, ScopeConfig.newBuilder().build()));
  }

  @Test
  void shouldReturnSecurityEventTypeContributionConfig() {
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .addContexts(TENANT_ID)
            .build();
    when(configServiceStub.getConfig(request))
        .thenReturn(
            GetConfigResponse.newBuilder()
                .setConfig(SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_1_VALUE)
                .build());

    assertEquals(
        SECURITY_EVENT_TYPE_CONTRIBUTION_1,
        securityEventTypeContributionManager.getSecurityEventTypeContribution(
            REQUEST_CONTEXT, ScopeConfig.newBuilder().build()));
  }

  @Test
  void shouldUpsertExistingSecurityEventTypeContributionConfig() {
    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .setConfig(SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_2_VALUE)
            .setContext("environment-id")
            .build();

    when(configServiceStub.upsertConfig(request))
        .thenReturn(
            UpsertConfigResponse.newBuilder()
                .setConfig(SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_2_VALUE)
                .build());

    assertEquals(
        SECURITY_EVENT_TYPE_CONTRIBUTION_2,
        securityEventTypeContributionManager.upsertSecurityEventTypeContribution(
            REQUEST_CONTEXT, SECURITY_EVENT_TYPE_CONTRIBUTION_2));
  }
}
