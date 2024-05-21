package ai.traceable.jira.integration.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.EncryptedData;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationConfigServiceGrpc;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationConfigServiceGrpc.JiraIntegrationConfigServiceBlockingStub;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationFilter;
import ai.traceable.jira.integration.config.service.api.v1.Scope;
import ai.traceable.jira.integration.config.service.api.v1.StringList;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
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
class JiraIntegrationConfigServiceImplTest {
  public static final String TENANT_ID = "default tenant";
  JiraIntegrationStore jiraIntegrationStore;
  MockGenericConfigService mockGenericConfigService;
  JiraIntegrationConfigServiceBlockingStub stub;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;

  @BeforeEach
  void setUp(TestInfo info) {
    initMockGenericConfigService(info);
    jiraIntegrationStore =
        new JiraIntegrationStore(
            ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel()),
            mockConfigChangeEventGenerator);
    mockGenericConfigService
        .addService(
            new JiraIntegrationConfigServiceImpl(
                new JiraIntegrationConfigServiceValidator(jiraIntegrationStore),
                new JiraIntegrationCoordinator(jiraIntegrationStore)))
        .start();
    stub = JiraIntegrationConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
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
  void getJiraIntegrations() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    JiraIntegration jiraIntegration2 = dummyJiraIntegration(2, "env2", "env3");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration2);

    GetJiraIntegrationsRequest getJiraIntegrationsRequest =
        GetJiraIntegrationsRequest.getDefaultInstance();
    assertEquals(2, stub.getJiraIntegrations(getJiraIntegrationsRequest).getIntegrationCount());

    GetJiraIntegrationsRequest getJiraIntegrationsRequest1 =
        GetJiraIntegrationsRequest.newBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env1"))))
            .build();
    assertEquals(
        jiraIntegration1, stub.getJiraIntegrations(getJiraIntegrationsRequest1).getIntegration(0));

    GetJiraIntegrationsRequest getJiraIntegrationsRequest2 =
        GetJiraIntegrationsRequest.newBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env2"))))
            .build();
    assertEquals(
        jiraIntegration2, stub.getJiraIntegrations(getJiraIntegrationsRequest2).getIntegration(0));

    GetJiraIntegrationsRequest getJiraIntegrationsRequest3 =
        GetJiraIntegrationsRequest.newBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env3"))))
            .build();
    assertEquals(
        jiraIntegration2, stub.getJiraIntegrations(getJiraIntegrationsRequest3).getIntegration(0));

    GetJiraIntegrationsRequest getJiraIntegrationsRequest4 =
        GetJiraIntegrationsRequest.newBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(
                                StringList.newBuilder().addAllValues(List.of("env1", "env2")))))
            .build();
    assertEquals(2, stub.getJiraIntegrations(getJiraIntegrationsRequest4).getIntegrationCount());

    // test that integration with no environments matches all
    JiraIntegration jiraIntegration3 = dummyJiraIntegration(3);
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration3);
    assertEquals(3, stub.getJiraIntegrations(getJiraIntegrationsRequest).getIntegrationCount());
    GetJiraIntegrationsRequest getJiraIntegrationsRequest5 =
        GetJiraIntegrationsRequest.newBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("newEnv"))))
            .build();
    assertEquals(
        jiraIntegration3, stub.getJiraIntegrations(getJiraIntegrationsRequest5).getIntegration(0));
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void createJiraIntegrationTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);

    // assert that new integration with overlapping of environment with existing integration throws
    // error
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);
    CreateJiraIntegrationRequest createJiraIntegrationRequest1 =
        dummyCreateJiraIntegrationRequest(1, "env1", "env2");
    assertThrows(
        StatusRuntimeException.class,
        () -> stub.createJiraIntegration(createJiraIntegrationRequest1));

    // this following integration without environments (valid for all environments) should also face
    // the overlap conflict and fail
    CreateJiraIntegrationRequest createJiraIntegrationRequest2 =
        dummyCreateJiraIntegrationRequest(1);
    assertThrows(
        StatusRuntimeException.class,
        () -> stub.createJiraIntegration(createJiraIntegrationRequest2));

    // this creation request shouldn't still succeed - there is no overlap with environments of an
    // existing
    // integration but the name of the integration already exists
    CreateJiraIntegrationRequest createJiraIntegrationRequest3 =
        dummyCreateJiraIntegrationRequest(1, "env2", "env3");
    assertThrows(
        StatusRuntimeException.class,
        () -> stub.createJiraIntegration(createJiraIntegrationRequest3));

    // this creation request should succeed - there is no overlap with environments of an existing
    // integration and the name is unique
    CreateJiraIntegrationRequest createJiraIntegrationRequest4 =
        dummyCreateJiraIntegrationRequest(2, "env2", "env3");
    JiraIntegration expectedJiraIntegration = dummyJiraIntegration(2, "env2", "env3");
    assertDoesNotThrow(() -> stub.createJiraIntegration(createJiraIntegrationRequest4));
    assertEquals(
        expectedJiraIntegration.toBuilder().clearId().build(),
        jiraIntegrationStore
            .getAllConfigData(
                requestContext,
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env2")))
                    .build())
            .get(0)
            .toBuilder()
            .clearId()
            .build());
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGet")
  @Tag("useMockGetAll")
  void updateJiraIntegrationTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    // trying to update a non-existent integration should throw an error
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest =
        UpdateJiraIntegrationRequest.newBuilder()
            .setJiraIntegrationId("wrongId")
            .setDescription("newDescription")
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> stub.updateJiraIntegration(updateJiraIntegrationRequest));

    // this should pass
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest1 =
        UpdateJiraIntegrationRequest.newBuilder()
            .setJiraIntegrationId(jiraIntegration1.getId())
            .setDescription("newDescription")
            .setName("newName")
            .setScope(
                Scope.newBuilder()
                    .setEnvironmentIds(
                        StringList.newBuilder().addAllValues(List.of("env2", "env3"))))
            .build();
    JiraIntegration expectedUpdatedJiraIntegration =
        jiraIntegration1.toBuilder()
            .setName(updateJiraIntegrationRequest1.getName())
            .setDescription(updateJiraIntegrationRequest1.getDescription())
            .setScope(updateJiraIntegrationRequest1.getScope())
            .build();
    assertDoesNotThrow(() -> stub.updateJiraIntegration(updateJiraIntegrationRequest1));
    assertEquals(
        expectedUpdatedJiraIntegration,
        jiraIntegrationStore.getData(requestContext, jiraIntegration1.getId()).orElseThrow());

    // this should successfully unset the scope
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest2 =
        updateJiraIntegrationRequest1.toBuilder().clearScope().build();
    stub.updateJiraIntegration(updateJiraIntegrationRequest2);
    assertFalse(
        jiraIntegrationStore
            .getData(requestContext, jiraIntegration1.getId())
            .orElseThrow()
            .hasScope());

    // this will fail because updating with the new scope will cause overlap in environments with
    // an existing integration
    JiraIntegration jiraIntegration2 = dummyJiraIntegration(2, "env4");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration2);
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest3 =
        UpdateJiraIntegrationRequest.newBuilder()
            .setJiraIntegrationId(jiraIntegration2.getId())
            .setName("newName")
            .setDescription("newDescription")
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> stub.updateJiraIntegration(updateJiraIntegrationRequest3));
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGet")
  @Tag("useMockDelete")
  void deleteJiraIntegrationTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    // Cannot delete a non-existent jira-integration should throw error
    DeleteJiraIntegrationRequest deleteJiraIntegrationRequest =
        DeleteJiraIntegrationRequest.newBuilder().setJiraIntegrationId("wrongId").build();
    assertThrows(
        StatusRuntimeException.class,
        () -> stub.deleteJiraIntegration(deleteJiraIntegrationRequest));

    // should not throw errors and succeed in deleting jiraIntegration1
    DeleteJiraIntegrationRequest deleteJiraIntegrationRequest1 =
        DeleteJiraIntegrationRequest.newBuilder()
            .setJiraIntegrationId(jiraIntegration1.getId())
            .build();
    assertDoesNotThrow(() -> stub.deleteJiraIntegration(deleteJiraIntegrationRequest1));
    assertFalse(jiraIntegrationStore.getData(requestContext, jiraIntegration1.getId()).isPresent());
  }

  private JiraIntegration dummyJiraIntegration(int sr, String... environmentId) {
    JiraIntegration.Builder jiraIntegration =
        JiraIntegration.newBuilder()
            .setId("dummyIntegrationId" + sr)
            .setName("dummyIntegration" + sr)
            .setDescription("dummyDescription")
            .setBaseUrl("dummyLoginUrl")
            .setConsumerKey("dummyConsumerKey")
            .setEncryptedAccessToken(
                EncryptedData.newBuilder()
                    .setBase64EncryptedValue("dummyEncryptedToken" + sr)
                    .setKeyId("keyId"));
    if (environmentId.length > 0) {
      jiraIntegration.setScope(
          Scope.newBuilder()
              .setEnvironmentIds(
                  StringList.newBuilder().addAllValues(Arrays.asList(environmentId))));
    }
    return jiraIntegration.build();
  }

  private CreateJiraIntegrationRequest dummyCreateJiraIntegrationRequest(
      int sr, String... environments) {
    // using values from dummyJiraIntegration(...) to ensure expected persisted integration
    // matches with the result of dummyJiraIntegration(...) - except the id which is created
    // randomly
    JiraIntegration jiraIntegration = dummyJiraIntegration(sr, environments);
    return CreateJiraIntegrationRequest.newBuilder()
        .setName(jiraIntegration.getName())
        .setDescription(jiraIntegration.getDescription())
        .setBaseUrl(jiraIntegration.getBaseUrl())
        .setConsumerKey(jiraIntegration.getConsumerKey())
        .setEncryptedAccessToken(jiraIntegration.getEncryptedAccessToken())
        .setScope(jiraIntegration.getScope())
        .build();
  }
}
