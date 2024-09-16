package ai.traceable.servicenow.itsm.integration.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.servicenow.itsm.integration.config.service.api.v1.CreateServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowIntegrationDetails;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmAuthCredentials;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegration;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationConfigServiceGrpc;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationConfigServiceGrpc.ServiceNowItsmIntegrationConfigServiceBlockingStub;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationFilter;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationScope;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationWithAuthCredentials;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.StringList;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ServiceNowItsmIntegrationConfigServiceImplTest {
  public static final String TENANT_ID = "tenantId";
  ServiceNowItsmIntegrationStore serviceNowItsmIntegrationStore;
  MockGenericConfigService mockGenericConfigService;
  ServiceNowItsmIntegrationConfigServiceBlockingStub stub;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;

  @BeforeEach
  void setup(TestInfo info) {
    initMockGenericConfigService(info);
    serviceNowItsmIntegrationStore =
        new ServiceNowItsmIntegrationStore(
            ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel()),
            mockConfigChangeEventGenerator);
    mockGenericConfigService
        .addService(
            new ServiceNowItsmIntegrationConfigServiceImpl(
                new ServiceNowItsmIntegrationConfigServiceValidator(serviceNowItsmIntegrationStore),
                new ServiceNowItsmIntegrationCoordinator(serviceNowItsmIntegrationStore)))
        .start();
    stub =
        ServiceNowItsmIntegrationConfigServiceGrpc.newBlockingStub(
            mockGenericConfigService.channel());
  }

  private void initMockGenericConfigService(TestInfo info) {
    mockGenericConfigService = new MockGenericConfigService();
    info.getTags()
        .forEach(
            tag -> {
              switch (tag) {
                case "useMockUpsert":
                  mockGenericConfigService.mockUpsert();
                  break;
                case "useMockGet":
                  mockGenericConfigService.mockGet();
                  break;
                case "useMockDelete":
                  mockGenericConfigService.mockDelete();
                  break;
                case "useMockGetAll":
                  mockGenericConfigService.mockGetAll();
                  break;
              }
            });
  }

  @AfterEach
  void tearDown() {
    mockGenericConfigService.shutdown();
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void getServiceNowItsmIntegration() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    ServiceNowItsmIntegrationWithAuthCredentials integrationWithAuthCredentials1 =
        dummyServiceNowWithAuthCredsIntegration(1, "env1");
    ServiceNowItsmIntegrationWithAuthCredentials integrationWithAuthCredentials2 =
        dummyServiceNowWithAuthCredsIntegration(2, "env2", "env3");
    ServiceNowItsmIntegration integration1 =
        ServiceNowItsmIntegration.newBuilder()
            .setId(integrationWithAuthCredentials1.getId())
            .setIntegrationDetails(integrationWithAuthCredentials1.getIntegrationDetails())
            .build();
    ServiceNowItsmIntegration integration2 =
        ServiceNowItsmIntegration.newBuilder()
            .setId(integrationWithAuthCredentials2.getId())
            .setIntegrationDetails(integrationWithAuthCredentials2.getIntegrationDetails())
            .build();

    serviceNowItsmIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials1);
    serviceNowItsmIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials2);
    GetServiceNowItsmIntegrationRequest request =
        GetServiceNowItsmIntegrationRequest.getDefaultInstance();
    assertEquals(2, stub.getServiceNowItsmIntegration(request).getServiceNowItsmIntegrationCount());

    GetServiceNowItsmIntegrationRequest request1 =
        GetServiceNowItsmIntegrationRequest.newBuilder()
            .setFilter(
                ServiceNowItsmIntegrationFilter.newBuilder()
                    .setScope(
                        ServiceNowItsmIntegrationScope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env1"))))
            .build();
    assertEquals(
        integration1, stub.getServiceNowItsmIntegration(request1).getServiceNowItsmIntegration(0));

    GetServiceNowItsmIntegrationRequest request2 =
        GetServiceNowItsmIntegrationRequest.newBuilder()
            .setFilter(
                ServiceNowItsmIntegrationFilter.newBuilder()
                    .setScope(
                        ServiceNowItsmIntegrationScope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env2"))))
            .build();
    assertEquals(
        integration2, stub.getServiceNowItsmIntegration(request2).getServiceNowItsmIntegration(0));

    GetServiceNowItsmIntegrationRequest request3 =
        GetServiceNowItsmIntegrationRequest.newBuilder()
            .setFilter(
                ServiceNowItsmIntegrationFilter.newBuilder()
                    .setScope(
                        ServiceNowItsmIntegrationScope.newBuilder()
                            .setEnvironmentIds(
                                StringList.newBuilder().addAllValues(List.of("env1", "env2")))))
            .build();
    assertEquals(
        2, stub.getServiceNowItsmIntegration(request3).getServiceNowItsmIntegrationCount());

    // test that integration with no environment matches all
    ServiceNowItsmIntegrationWithAuthCredentials integrationWithAuthCredentials3 =
        dummyServiceNowWithAuthCredsIntegration(3);
    serviceNowItsmIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials3);
    assertEquals(
        3, stub.getServiceNowItsmIntegration(request3).getServiceNowItsmIntegrationCount());
    ServiceNowItsmIntegration integration3 =
        ServiceNowItsmIntegration.newBuilder()
            .setId(integrationWithAuthCredentials3.getId())
            .setIntegrationDetails(integrationWithAuthCredentials3.getIntegrationDetails())
            .build();
    GetServiceNowItsmIntegrationRequest request4 =
        GetServiceNowItsmIntegrationRequest.newBuilder()
            .setFilter(
                ServiceNowItsmIntegrationFilter.newBuilder()
                    .setScope(
                        ServiceNowItsmIntegrationScope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("newEnv"))))
            .build();
    assertEquals(
        integration3, stub.getServiceNowItsmIntegration(request4).getServiceNowItsmIntegration(0));
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void createServiceNowItsmIntegration() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    // assert that new integration with overlapping of environment with existing integration throws
    // error
    ServiceNowItsmIntegrationWithAuthCredentials integrationWithAuthCredentials1 =
        dummyServiceNowWithAuthCredsIntegration(1, "env1");
    ServiceNowItsmIntegration integration1 =
        ServiceNowItsmIntegration.newBuilder()
            .setId(integrationWithAuthCredentials1.getId())
            .setIntegrationDetails(integrationWithAuthCredentials1.getIntegrationDetails())
            .build();
    serviceNowItsmIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials1);
    CreateServiceNowItsmIntegrationRequest request1 =
        dummyCreateServiceIntegrationRequest(1, "env1", "env2");
    assertThrows(
        StatusRuntimeException.class, () -> stub.createServiceNowItsmIntegration(request1));

    // this following integration without environments (valid for all environments) should also face
    // the overlap conflict and fail
    CreateServiceNowItsmIntegrationRequest request2 = dummyCreateServiceIntegrationRequest(1);
    assertThrows(
        StatusRuntimeException.class, () -> stub.createServiceNowItsmIntegration(request2));

    // this creation request shouldn't still succeed - there is no overlap with environments of an
    // existing
    // integration but the name of the integration already exists
    CreateServiceNowItsmIntegrationRequest request3 =
        dummyCreateServiceIntegrationRequest(1, "env2", "env3");
    assertThrows(
        StatusRuntimeException.class, () -> stub.createServiceNowItsmIntegration(request3));

    // this creation request should succeed - there is no overlap with environments of an existing
    // integration and the name is unique
    CreateServiceNowItsmIntegrationRequest request4 =
        dummyCreateServiceIntegrationRequest(2, "env2", "env3");
    ServiceNowItsmIntegration expectedIntegration = dummyServiceNowIntegration(2, "env2", "env3");
    assertDoesNotThrow(() -> stub.createServiceNowItsmIntegration(request4));
    assertEquals(
        expectedIntegration.toBuilder().clearId().build().toString(),
        serviceNowItsmIntegrationStore
            .getAllConfigData(
                requestContext,
                ServiceNowItsmIntegrationFilter.newBuilder()
                    .setScope(
                        ServiceNowItsmIntegrationScope.newBuilder()
                            .setEnvironmentIds(
                                StringList.newBuilder().addAllValues(List.of("env3"))))
                    .build())
            .get(0)
            .toBuilder()
            .clearId()
            .clearAuthCredentials()
            .build()
            .toString());
  }

  private ServiceNowItsmIntegrationWithAuthCredentials dummyServiceNowWithAuthCredsIntegration(
      int sr, String... environmentId) {
    ServiceNowItsmIntegrationWithAuthCredentials.Builder serviceNowIntegration =
        ServiceNowItsmIntegrationWithAuthCredentials.newBuilder()
            .setId("dummyIntegrationId" + sr)
            .setAuthCredentials(
                ServiceNowItsmAuthCredentials.newBuilder()
                    .setUserName("dummyUsername")
                    .setEncryptedUserPassword("dummyPassword")
                    .setClientId("dummyClientId")
                    .setEncryptedClientSecret("dummyClientSecret")
                    .setEncryptionKeyId("dummyKyeId"))
            .setIntegrationDetails(
                ServiceNowIntegrationDetails.newBuilder()
                    .setName("dummyIntegration" + sr)
                    .setDescription("dummyDescription")
                    .setServerUrl("dummyServerUrl"));

    if (environmentId.length > 0) {
      serviceNowIntegration
          .getIntegrationDetailsBuilder()
          .setScope(
              ServiceNowItsmIntegrationScope.newBuilder()
                  .setEnvironmentIds(
                      StringList.newBuilder().addAllValues(Arrays.asList(environmentId))));
    }
    return serviceNowIntegration.build();
  }

  private ServiceNowItsmIntegration dummyServiceNowIntegration(int sr, String... environmentId) {
    ServiceNowItsmIntegration.Builder serviceNowItsmIntegration =
        ServiceNowItsmIntegration.newBuilder()
            .setId("dummyIntegrationId" + sr)
            .setIntegrationDetails(
                ServiceNowIntegrationDetails.newBuilder()
                    .setName("dummyIntegration" + sr)
                    .setDescription("dummyDescription")
                    .setServerUrl("dummyServerUrl"));

    if (environmentId.length > 0) {
      serviceNowItsmIntegration
          .getIntegrationDetailsBuilder()
          .setScope(
              ServiceNowItsmIntegrationScope.newBuilder()
                  .setEnvironmentIds(
                      StringList.newBuilder().addAllValues(Arrays.asList(environmentId))));
    }

    return serviceNowItsmIntegration.build();
  }

  private CreateServiceNowItsmIntegrationRequest dummyCreateServiceIntegrationRequest(
      int sr, String... environmentId) {
    ServiceNowItsmIntegrationWithAuthCredentials serviceNowIntegration =
        dummyServiceNowWithAuthCredsIntegration(sr, environmentId);
    return CreateServiceNowItsmIntegrationRequest.newBuilder()
        .setName(serviceNowIntegration.getIntegrationDetails().getName())
        .setDescription(serviceNowIntegration.getIntegrationDetails().getDescription())
        .setServerUrl(serviceNowIntegration.getIntegrationDetails().getServerUrl())
        .setScope(serviceNowIntegration.getIntegrationDetails().getScope())
        .setAuthCredentials(serviceNowIntegration.getAuthCredentials())
        .build();
  }
}
