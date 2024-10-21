package ai.traceable.azure.devops.integration.config.service;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.azure.devops.integration.config.service.api.v1.*;
import ai.traceable.azure.devops.integration.config.service.api.v1.AzureDevopsIntegrationConfigServiceGrpc.AzureDevopsIntegrationConfigServiceBlockingStub;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AzureDevopsIntegrationConfigServiceImplTest {
  public static final String TENANT_ID = "tenantId";
  AzureDevopsIntegrationStore azureDevopsIntegrationStore;
  MockGenericConfigService mockGenericConfigService;
  AzureDevopsIntegrationConfigServiceBlockingStub stub;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;

  @BeforeEach
  void setup(TestInfo info) {
    initMockGenericConfigService(info);
    azureDevopsIntegrationStore =
        new AzureDevopsIntegrationStore(
            ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel()),
            mockConfigChangeEventGenerator);
    mockGenericConfigService
        .addService(
            new AzureDevopsIntegrationConfigServiceImpl(
                new AzureDevopsIntegrationConfigServiceValidator(azureDevopsIntegrationStore),
                new AzureDevopsIntegrationCoordinator(azureDevopsIntegrationStore)))
        .start();
    stub =
        AzureDevopsIntegrationConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
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
  void getAzureDevopsIntegrations() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    AzureDevopsIntegrationWithAuthCredentials integrationWithAuthCredentials1 =
        dummyAzureDevopsIntegrationWithAuthCreds(1, "env1");
    AzureDevopsIntegrationWithAuthCredentials integrationWithAuthCredentials2 =
        dummyAzureDevopsIntegrationWithAuthCreds(2, "env2", "env3");
    AzureDevopsIntegration integration1 =
        AzureDevopsIntegration.newBuilder()
            .setId(integrationWithAuthCredentials1.getId())
            .setAzureDevopsIntegrationDetails(
                integrationWithAuthCredentials1.getIntegrationDetails())
            .build();
    AzureDevopsIntegration integration2 =
        AzureDevopsIntegration.newBuilder()
            .setId(integrationWithAuthCredentials2.getId())
            .setAzureDevopsIntegrationDetails(
                integrationWithAuthCredentials2.getIntegrationDetails())
            .build();

    azureDevopsIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials1);
    azureDevopsIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials2);
    GetAzureDevopsIntegrationsRequest request =
        GetAzureDevopsIntegrationsRequest.getDefaultInstance();
    assertEquals(2, stub.getAzureDevopsIntegrations(request).getAzureDevopsIntegrationCount());

    GetAzureDevopsIntegrationsRequest request1 =
        GetAzureDevopsIntegrationsRequest.newBuilder()
            .setFilter(
                AzureDevopsIntegrationFilter.newBuilder()
                    .setScope(
                        AzureDevopsIntegrationScope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env1"))))
            .build();
    GetAzureDevopsIntegrationsResponse response1 = stub.getAzureDevopsIntegrations(request1);
    assertEquals(1, response1.getAzureDevopsIntegrationCount());
    assertEquals(integration1, response1.getAzureDevopsIntegration(0));

    GetAzureDevopsIntegrationsRequest request2 =
        GetAzureDevopsIntegrationsRequest.newBuilder()
            .setFilter(
                AzureDevopsIntegrationFilter.newBuilder()
                    .setScope(
                        AzureDevopsIntegrationScope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env2"))))
            .build();
    GetAzureDevopsIntegrationsResponse response2 = stub.getAzureDevopsIntegrations(request2);
    assertEquals(1, response2.getAzureDevopsIntegrationCount());
    assertEquals(integration2, response2.getAzureDevopsIntegration(0));

    GetAzureDevopsIntegrationsRequest request3 =
        GetAzureDevopsIntegrationsRequest.newBuilder()
            .setFilter(
                AzureDevopsIntegrationFilter.newBuilder()
                    .setScope(
                        AzureDevopsIntegrationScope.newBuilder()
                            .setEnvironmentIds(
                                StringList.newBuilder().addAllValues(List.of("env1", "env2")))))
            .build();
    assertEquals(2, stub.getAzureDevopsIntegrations(request3).getAzureDevopsIntegrationCount());

    // test that integration with no environment matches all
    AzureDevopsIntegrationWithAuthCredentials integrationWithAuthCredentials3 =
        dummyAzureDevopsIntegrationWithAuthCreds(3);
    azureDevopsIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials3);
    assertEquals(3, stub.getAzureDevopsIntegrations(request3).getAzureDevopsIntegrationCount());
    AzureDevopsIntegration integration3 =
        AzureDevopsIntegration.newBuilder()
            .setId(integrationWithAuthCredentials3.getId())
            .setAzureDevopsIntegrationDetails(
                integrationWithAuthCredentials3.getIntegrationDetails())
            .build();
    GetAzureDevopsIntegrationsRequest request4 =
        GetAzureDevopsIntegrationsRequest.newBuilder()
            .setFilter(
                AzureDevopsIntegrationFilter.newBuilder()
                    .setScope(
                        AzureDevopsIntegrationScope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("newEnv"))))
            .build();
    GetAzureDevopsIntegrationsResponse response4 = stub.getAzureDevopsIntegrations(request4);
    assertEquals(1, response4.getAzureDevopsIntegrationCount());
    assertEquals(
        integration3, stub.getAzureDevopsIntegrations(request4).getAzureDevopsIntegration(0));
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void createAzureDevopsIntegration() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    // assert that new integration with overlapping of environment with existing integration throws
    // error
    AzureDevopsIntegrationWithAuthCredentials integrationWithAuthCredentials1 =
        dummyAzureDevopsIntegrationWithAuthCreds(1, "env1");

    azureDevopsIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials1);
    CreateAzureDevopsIntegrationRequest request1 =
        dummmyCreateAzureDevopsIntegrationRequest(1, "env1", "env2");
    assertThrows(StatusRuntimeException.class, () -> stub.createAzureDevopsIntegration(request1));

    // this following integration without environments (valid for all environments) should also face
    // the overlap conflict and fail
    CreateAzureDevopsIntegrationRequest request2 = dummmyCreateAzureDevopsIntegrationRequest(1);
    assertThrows(StatusRuntimeException.class, () -> stub.createAzureDevopsIntegration(request2));

    // this creation request shouldn't still succeed - there is no overlap with environments of an
    // existing
    // integration but the name of the integration already exists
    CreateAzureDevopsIntegrationRequest request3 =
        dummmyCreateAzureDevopsIntegrationRequest(1, "env2", "env3");
    assertThrows(StatusRuntimeException.class, () -> stub.createAzureDevopsIntegration(request3));

    // this creation request should succeed - there is no overlap with environments of an existing
    // integration and the name is unique
    CreateAzureDevopsIntegrationRequest request4 =
        dummmyCreateAzureDevopsIntegrationRequest(2, "env2", "env3");
    AzureDevopsIntegration expectedIntegration = dummyAzureDevopsIntegration(2, "env2", "env3");
    assertDoesNotThrow(() -> stub.createAzureDevopsIntegration(request4));
    assertEquals(
        expectedIntegration.toBuilder().getAzureDevopsIntegrationDetails(),
        azureDevopsIntegrationStore
            .getAllConfigData(
                requestContext,
                AzureDevopsIntegrationFilter.newBuilder()
                    .setScope(
                        AzureDevopsIntegrationScope.newBuilder()
                            .setEnvironmentIds(
                                StringList.newBuilder().addAllValues(List.of("env3"))))
                    .build())
            .get(0)
            .toBuilder()
            .getIntegrationDetails());
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  @Tag("useMockGet")
  void updateAzureDevopsIntegration() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    // assert that update of integration with non-existing integration_id throws
    // error
    AzureDevopsIntegrationWithAuthCredentials integrationWithAuthCredentials1 =
        dummyAzureDevopsIntegrationWithAuthCreds(1, "env1");

    azureDevopsIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials1);
    UpdateAzureDevopsIntegrationRequest request =
        dummyUpdateAzureDevopsIntegrationRequest(2, "env1", "env2");
    assertThrows(StatusRuntimeException.class, () -> stub.updateAzureDevopsIntegration(request));

    // this following update integration without environments (valid for all environments) should
    // also face
    // the overlap conflict and fail
    AzureDevopsIntegrationWithAuthCredentials integrationWithAuthCredentials2 =
        dummyAzureDevopsIntegrationWithAuthCreds(2, "env2");
    azureDevopsIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials2);
    UpdateAzureDevopsIntegrationRequest request1 = dummyUpdateAzureDevopsIntegrationRequest(1);
    assertThrows(StatusRuntimeException.class, () -> stub.updateAzureDevopsIntegration(request1));

    // this following update integration would pass (all required fields present, valid id)
    UpdateAzureDevopsIntegrationRequest request2 =
        dummyUpdateAzureDevopsIntegrationRequest(2, "env3");
    assertDoesNotThrow(() -> stub.updateAzureDevopsIntegration(request2));

    // update all fields and assert updated fields
    // update for sr 2
    AzureDevopsIntegrationAuthCredentials updatedAuthCredentials =
        AzureDevopsIntegrationAuthCredentials.newBuilder()
            .setEncryptionKeyId("updatedKeyId")
            .setEncryptedPat("updatedPAT")
            .build();

    AzureDevopsIntegrationDetails updatedIntegrationDetails =
        AzureDevopsIntegrationDetails.newBuilder()
            .setName("updatedIntegration")
            .setDescription("updatedDescription")
            .setOrganizationUrl("updatedUrl")
            .build();

    AzureDevopsIntegrationScope updatedIntegrationScope =
        AzureDevopsIntegrationScope.newBuilder()
            .setEnvironmentIds(StringList.newBuilder().addAllValues(Arrays.asList("env3", "env4")))
            .build();

    AzureDevopsIntegrationWithAuthCredentials customIntegration =
        dummyAzureDevopsIntegrationWithAuthCreds(
            2, updatedAuthCredentials, updatedIntegrationDetails, updatedIntegrationScope);

    UpdateAzureDevopsIntegrationRequest request3 =
        UpdateAzureDevopsIntegrationRequest.newBuilder()
            .setIntegrationId(customIntegration.getId())
            .setName(customIntegration.getIntegrationDetails().getName())
            .setDescription(customIntegration.getIntegrationDetails().getDescription())
            .setOrganizationUrl(customIntegration.getIntegrationDetails().getOrganizationUrl())
            .setAzureDevopsIntegrationScope(
                customIntegration.getIntegrationDetails().getAzureDevopsIntegrationScope())
            .setAzureDevopsIntegrationAuthCredentials(customIntegration.getAuthCredentials())
            .build();
    UpdateAzureDevopsIntegrationResponse updateAzureDevopsIntegrationResponse =
        stub.updateAzureDevopsIntegration(request3);
    GetAzureDevopsIntegrationWithAuthCredentialsRequest request4 =
        GetAzureDevopsIntegrationWithAuthCredentialsRequest.newBuilder()
            .setIntegrationId(request3.getIntegrationId())
            .build();
    GetAzureDevopsIntegrationWithAuthCredentialsResponse
        getAzureDevopsIntegrationWithAuthCredentialsResponse =
            stub.getAzureDevopsIntegrationWithAuthCredentials(request4);
    assertEquals(
        customIntegration,
        getAzureDevopsIntegrationWithAuthCredentialsResponse
            .getAzureDevopsIntegrationWithAuthCredentials());
    assertEquals(
        customIntegration.getIntegrationDetails(),
        getAzureDevopsIntegrationWithAuthCredentialsResponse
            .getAzureDevopsIntegrationWithAuthCredentials()
            .getIntegrationDetails());
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockDelete")
  void deleteAzureDevopsIntegration() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    // assert that deletion of integration with non-existing integration_id throws
    // error
    AzureDevopsIntegrationWithAuthCredentials integrationWithAuthCredentials1 =
        dummyAzureDevopsIntegrationWithAuthCreds(1, "env1");
    azureDevopsIntegrationStore.upsertObject(requestContext, integrationWithAuthCredentials1);

    DeleteAzureDevopsIntegrationRequest request =
        DeleteAzureDevopsIntegrationRequest.newBuilder().setIntegrationId("invalidId").build();
    assertThrows(StatusRuntimeException.class, () -> stub.deleteAzureDevopsIntegration(request));

    // assert that deletion of integration with default delete object throws
    // error
    DeleteAzureDevopsIntegrationRequest request1 =
        DeleteAzureDevopsIntegrationRequest.getDefaultInstance();
    assertThrows(StatusRuntimeException.class, () -> stub.deleteAzureDevopsIntegration(request1));

    // assertion that deletion of integration with existing integration id passed
    DeleteAzureDevopsIntegrationRequest request2 =
        DeleteAzureDevopsIntegrationRequest.newBuilder()
            .setIntegrationId(integrationWithAuthCredentials1.getId())
            .build();
    assertDoesNotThrow(() -> stub.deleteAzureDevopsIntegration(request2));
  }

  private AzureDevopsIntegrationWithAuthCredentials dummyAzureDevopsIntegrationWithAuthCreds(
      int sr, String... environmentId) {
    AzureDevopsIntegrationWithAuthCredentials.Builder azureDevopsIntegration =
        AzureDevopsIntegrationWithAuthCredentials.newBuilder()
            .setId("dummyIntegrationId" + sr)
            .setAuthCredentials(
                AzureDevopsIntegrationAuthCredentials.newBuilder()
                    .setEncryptionKeyId("dummyKyeId")
                    .setEncryptedPat("dummyPAT"))
            .setIntegrationDetails(
                AzureDevopsIntegrationDetails.newBuilder()
                    .setName("dummyIntegration" + sr)
                    .setDescription("dummyDescription")
                    .setOrganizationUrl("dummyOrganizationUrl"));

    if (environmentId.length > 0) {
      azureDevopsIntegration
          .getIntegrationDetailsBuilder()
          .setAzureDevopsIntegrationScope(
              AzureDevopsIntegrationScope.newBuilder()
                  .setEnvironmentIds(
                      StringList.newBuilder().addAllValues(Arrays.asList(environmentId))));
    }
    return azureDevopsIntegration.build();
  }

  private AzureDevopsIntegrationWithAuthCredentials dummyAzureDevopsIntegrationWithAuthCreds(
      int sr,
      AzureDevopsIntegrationAuthCredentials authCredentials,
      AzureDevopsIntegrationDetails integrationDetails,
      AzureDevopsIntegrationScope azureDevopsIntegrationScope) {

    // Build the base AzureDevopsIntegrationWithAuthCredentials object
    AzureDevopsIntegrationWithAuthCredentials.Builder azureDevopsIntegration =
        AzureDevopsIntegrationWithAuthCredentials.newBuilder().setId("dummyIntegrationId" + sr);

    // Set authentication credentials if provided
    if (authCredentials != null) {
      azureDevopsIntegration.setAuthCredentials(authCredentials);
    } else {
      // Set default auth credentials
      azureDevopsIntegration.setAuthCredentials(
          AzureDevopsIntegrationAuthCredentials.newBuilder()
              .setEncryptionKeyId("defaultKeyId")
              .setEncryptedPat("defaultPAT")
              .build());
    }

    // Set integration details if provided
    if (integrationDetails != null) {
      azureDevopsIntegration.setIntegrationDetails(integrationDetails);
    } else {
      // Set default integration details
      azureDevopsIntegration.setIntegrationDetails(
          AzureDevopsIntegrationDetails.newBuilder()
              .setName("defaultIntegration" + sr)
              .setDescription("defaultDescription")
              .setOrganizationUrl("defaultOrganizationUrl")
              .build());
    }

    // Set Azure DevOps integration scope if provided
    if (azureDevopsIntegrationScope != null) {
      azureDevopsIntegration
          .getIntegrationDetailsBuilder()
          .setAzureDevopsIntegrationScope(azureDevopsIntegrationScope);
    }

    // Build and return the object
    return azureDevopsIntegration.build();
  }

  private AzureDevopsIntegration dummyAzureDevopsIntegration(int sr, String... environmentId) {
    AzureDevopsIntegration.Builder azureDevopsIntegration =
        AzureDevopsIntegration.newBuilder()
            .setId("dummyIntegrationId" + sr)
            .setAzureDevopsIntegrationDetails(
                AzureDevopsIntegrationDetails.newBuilder()
                    .setName("dummyIntegration" + sr)
                    .setDescription("dummyDescription")
                    .setOrganizationUrl("dummyOrganizationUrl"));

    if (environmentId.length > 0) {
      azureDevopsIntegration
          .getAzureDevopsIntegrationDetailsBuilder()
          .setAzureDevopsIntegrationScope(
              AzureDevopsIntegrationScope.newBuilder()
                  .setEnvironmentIds(
                      StringList.newBuilder().addAllValues(Arrays.asList(environmentId))));
    }

    return azureDevopsIntegration.build();
  }

  private CreateAzureDevopsIntegrationRequest dummmyCreateAzureDevopsIntegrationRequest(
      int sr, String... environmentId) {
    AzureDevopsIntegrationWithAuthCredentials azureDevopsIntegration =
        dummyAzureDevopsIntegrationWithAuthCreds(sr, environmentId);
    return CreateAzureDevopsIntegrationRequest.newBuilder()
        .setName(azureDevopsIntegration.getIntegrationDetails().getName())
        .setDescription(azureDevopsIntegration.getIntegrationDetails().getDescription())
        .setOrganizationUrl(azureDevopsIntegration.getIntegrationDetails().getOrganizationUrl())
        .setAzureDevopsIntegrationScope(
            azureDevopsIntegration.getIntegrationDetails().getAzureDevopsIntegrationScope())
        .setAzureDevopsIntegrationAuthCredentials(azureDevopsIntegration.getAuthCredentials())
        .build();
  }

  private UpdateAzureDevopsIntegrationRequest dummyUpdateAzureDevopsIntegrationRequest(
      int sr, String... environmentId) {
    AzureDevopsIntegrationWithAuthCredentials azureDevopsIntegration =
        dummyAzureDevopsIntegrationWithAuthCreds(sr, environmentId);
    return UpdateAzureDevopsIntegrationRequest.newBuilder()
        .setIntegrationId(azureDevopsIntegration.getId())
        .setName(azureDevopsIntegration.getIntegrationDetails().getName())
        .setDescription(azureDevopsIntegration.getIntegrationDetails().getDescription())
        .setOrganizationUrl(azureDevopsIntegration.getIntegrationDetails().getOrganizationUrl())
        .setAzureDevopsIntegrationScope(
            azureDevopsIntegration.getIntegrationDetails().getAzureDevopsIntegrationScope())
        .setAzureDevopsIntegrationAuthCredentials(azureDevopsIntegration.getAuthCredentials())
        .build();
  }
}
