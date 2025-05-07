package ai.traceable.jira.integration.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.EncryptedData;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsResponse;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsFilter;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsResponse;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationConfigServiceGrpc;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationConfigServiceGrpc.JiraIntegrationConfigServiceBlockingStub;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationFilter;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfigurationDetails;
import ai.traceable.jira.integration.config.service.api.v1.JiraStatusMapping;
import ai.traceable.jira.integration.config.service.api.v1.JiraStatusMappingConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.Scope;
import ai.traceable.jira.integration.config.service.api.v1.StringList;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityStatus;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityType;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateProjectIssueConfigurationRequest;
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
  private final String PROJECT_ID = "project_id";
  private final String ISSUE_TYPE = "issue_type";
  private final String CONFIG_ID = "config_id";
  JiraIntegrationStore jiraIntegrationStore;
  JiraAdditionalConfigurationStore jiraAdditionalConfigurationStore;
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
    jiraAdditionalConfigurationStore =
        new JiraAdditionalConfigurationStore(
            ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel()),
            mockConfigChangeEventGenerator);
    mockGenericConfigService
        .addService(
            new JiraIntegrationConfigServiceImpl(
                new JiraIntegrationConfigServiceValidator(jiraIntegrationStore),
                new JiraIntegrationCoordinator(
                    jiraIntegrationStore,
                    new JiraAdditionalConfigurationCoordinator(jiraAdditionalConfigurationStore),
                    jiraAdditionalConfigurationStore)))
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
                case "useMockUpsertAll":
                  mockGenericConfigService.mockUpsertAll();
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
                case "useMockDeleteAll":
                  mockGenericConfigService.mockDeleteAll();
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
  @Tag("useMockUpsertAll")
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
        jiraIntegration1.toBuilder().setJiraBidirectionalSyncIsEnabled(false).build(),
        stub.getJiraIntegrations(getJiraIntegrationsRequest1).getIntegration(0));

    GetJiraIntegrationsRequest getJiraIntegrationsRequest2 =
        GetJiraIntegrationsRequest.newBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env2"))))
            .build();
    assertEquals(
        jiraIntegration2.toBuilder().setJiraBidirectionalSyncIsEnabled(false).build(),
        stub.getJiraIntegrations(getJiraIntegrationsRequest2).getIntegration(0));

    GetJiraIntegrationsRequest getJiraIntegrationsRequest3 =
        GetJiraIntegrationsRequest.newBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues("env3"))))
            .build();
    assertEquals(
        jiraIntegration2.toBuilder().setJiraBidirectionalSyncIsEnabled(false).build(),
        stub.getJiraIntegrations(getJiraIntegrationsRequest3).getIntegration(0));

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
        jiraIntegration3.toBuilder().setJiraBidirectionalSyncIsEnabled(false).build(),
        stub.getJiraIntegrations(getJiraIntegrationsRequest5).getIntegration(0));

    // test that if bidirectional config found, then correct status is returned
    JiraStatusMapping jiraStatusMappingCommon1 =
        CreateJiraStatusMapping(
            "Under Review", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_ISSUES_COMMON_FIXED);
    JiraProjectIssueConfiguration jiraProjectIssueConfiguration =
        createDummyJiraProjectIssueConfiguration(
            List.of(jiraStatusMappingCommon1), jiraIntegration2.getId());

    JiraProjectIssueConfiguration jiraProjectIssueConfiguration1 =
        createDummyJiraProjectIssueConfiguration(
            List.of(jiraStatusMappingCommon1), jiraIntegration3.getId());

    JiraProjectIssueConfiguration jiraProjectIssueConfiguration2 =
        createDummyJiraProjectIssueConfiguration(
            List.of(jiraStatusMappingCommon1), jiraIntegration3.getId(), false);

    JiraProjectIssueConfiguration jiraProjectIssueConfiguration3 =
        createDummyJiraProjectIssueConfiguration(
            List.of(jiraStatusMappingCommon1), jiraIntegration1.getId(), false);

    jiraAdditionalConfigurationStore.upsertObjects(
        requestContext,
        List.of(
            jiraProjectIssueConfiguration,
            jiraProjectIssueConfiguration1,
            jiraProjectIssueConfiguration2,
            jiraProjectIssueConfiguration3));

    GetJiraIntegrationsRequest getJiraIntegrationsRequest6 =
        GetJiraIntegrationsRequest.newBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(
                                StringList.newBuilder()
                                    .addAllValues(List.of("env2", "newEnv", "env1")))))
            .build();
    GetJiraIntegrationsResponse getJiraIntegrationsResponse =
        stub.getJiraIntegrations(getJiraIntegrationsRequest6);
    System.out.println(getJiraIntegrationsResponse);
    assertEquals(3, getJiraIntegrationsResponse.getIntegrationCount());

    assertFalse(
        getJiraIntegrationsResponse.getIntegrationList().stream()
            .filter(integration -> jiraIntegration1.getId().equals(integration.getId()))
            .findFirst()
            .get()
            .getJiraBidirectionalSyncIsEnabled());

    assertTrue(
        getJiraIntegrationsResponse.getIntegrationList().stream()
            .filter(integration -> jiraIntegration2.getId().equals(integration.getId()))
            .findFirst()
            .get()
            .getJiraBidirectionalSyncIsEnabled());

    assertTrue(
        getJiraIntegrationsResponse.getIntegrationList().stream()
            .filter(integration -> jiraIntegration3.getId().equals(integration.getId()))
            .findFirst()
            .get()
            .getJiraBidirectionalSyncIsEnabled());
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

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void createProjectIssueConfigurationTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    JiraStatusMapping jiraStatusMapping1 =
        CreateJiraStatusMapping(
            "Task", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_ISSUES_COMMON_FIXED);
    JiraStatusMapping jiraStatusMapping2 =
        CreateJiraStatusMapping(
            "Bug", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_ISSUES_COMMON_UNDER_REVIEW);
    CreateProjectIssueConfigurationRequest request =
        CreateProjectIssueConfigurationRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setJiraStatusMappingConfiguration(
                JiraStatusMappingConfiguration.newBuilder()
                    .addAllStatusMappings(List.of(jiraStatusMapping1, jiraStatusMapping2))
                    .build())
            .setJiraBidirectionalSyncIsEnabled(true)
            .build();
    assertDoesNotThrow(() -> stub.createProjectIssueConfiguration(request));
    JiraProjectIssueConfiguration expectedJiraProjectIssueConfiguration =
        createDummyJiraProjectIssueConfiguration(
            List.of(jiraStatusMapping1, jiraStatusMapping2), request.getIntegrationId());
    assertEquals(
        expectedJiraProjectIssueConfiguration.toBuilder().clearConfigurationId().build(),
        jiraAdditionalConfigurationStore
            .getAllConfigData(
                requestContext,
                GetProjectIssueConfigurationsFilter.newBuilder()
                    .setIntegrationId(jiraIntegration1.getId())
                    .setProjectId(PROJECT_ID)
                    .setIssueType(ISSUE_TYPE)
                    .build())
            .get(0)
            .toBuilder()
            .clearConfigurationId()
            .build());
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void getProjectIssueConfigurationTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(2, "env2");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    JiraStatusMapping jiraStatusMappingCommon1 =
        CreateJiraStatusMapping(
            "Under Review", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_ISSUES_COMMON_FIXED);
    JiraStatusMapping jiraStatusMappingCommon2 =
        CreateJiraStatusMapping(
            "Open", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_ISSUES_COMMON_UNDER_REVIEW);
    JiraStatusMapping jiraStatusMappingSpecificEntityType1 =
        CreateJiraStatusMapping(
            "To do", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_AST_VULNERABILITY_ACCEPTED_RISK);
    JiraStatusMapping jiraStatusMappingSpecificEntityType2 =
        CreateJiraStatusMapping(
            "Done", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_AST_VULNERABILITY_FIXED);

    JiraProjectIssueConfiguration jiraProjectIssueConfiguration =
        createDummyJiraProjectIssueConfiguration(
            List.of(
                jiraStatusMappingCommon1,
                jiraStatusMappingCommon2,
                jiraStatusMappingSpecificEntityType1,
                jiraStatusMappingSpecificEntityType2),
            jiraIntegration1.getId());
    jiraAdditionalConfigurationStore.upsertObject(requestContext, jiraProjectIssueConfiguration);

    // get mappings for specific entity type
    GetProjectIssueConfigurationsResponse response =
        stub.getProjectIssueConfigurations(
            GetProjectIssueConfigurationsRequest.newBuilder()
                .setFilter(
                    GetProjectIssueConfigurationsFilter.newBuilder()
                        .setIntegrationId(jiraIntegration1.getId())
                        .setProjectId(PROJECT_ID)
                        .setIssueType(ISSUE_TYPE)
                        .addAllSupportedEntityTypes(
                            List.of(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY))
                        .build())
                .build());
    assertEquals(1, response.getJiraProjectConfigurationsCount());
    assertEquals(
        jiraProjectIssueConfiguration.toBuilder()
            .clearConfigurationId()
            .setJiraProjectIssueConfigurationDetails(
                jiraProjectIssueConfiguration.getJiraProjectIssueConfigurationDetails().toBuilder()
                    .setJiraStatusMappingConfiguration(
                        JiraStatusMappingConfiguration.newBuilder()
                            .addAllStatusMappings(
                                List.of(
                                    jiraStatusMappingSpecificEntityType1,
                                    jiraStatusMappingSpecificEntityType2))
                            .build()))
            .build(),
        response.getJiraProjectConfigurations(0).toBuilder().clearConfigurationId().build());

    // get all mappings when entity type not provided
    GetProjectIssueConfigurationsResponse response1 =
        stub.getProjectIssueConfigurations(
            GetProjectIssueConfigurationsRequest.newBuilder()
                .setFilter(
                    GetProjectIssueConfigurationsFilter.newBuilder()
                        .setIntegrationId(jiraIntegration1.getId())
                        .setProjectId(PROJECT_ID)
                        .setIssueType(ISSUE_TYPE)
                        .build())
                .build());
    assertEquals(1, response1.getJiraProjectConfigurationsCount());
    assertEquals(
        jiraProjectIssueConfiguration.getJiraProjectIssueConfigurationDetails(),
        response1.getJiraProjectConfigurations(0).toBuilder()
            .clearConfigurationId()
            .getJiraProjectIssueConfigurationDetails());

    // get common mappings if entity type provided but entity type specific mappings not found
    GetProjectIssueConfigurationsResponse response2 =
        stub.getProjectIssueConfigurations(
            GetProjectIssueConfigurationsRequest.newBuilder()
                .setFilter(
                    GetProjectIssueConfigurationsFilter.newBuilder()
                        .setIntegrationId(jiraIntegration1.getId())
                        .setProjectId(PROJECT_ID)
                        .setIssueType(ISSUE_TYPE)
                        .addAllSupportedEntityTypes(
                            List.of(TraceableEntityType.TRACEABLE_ENTITY_TYPE_THREAT_ACTIVITY))
                        .build())
                .build());
    assertEquals(1, response2.getJiraProjectConfigurationsCount());
    assertEquals(
        jiraProjectIssueConfiguration.toBuilder()
            .clearConfigurationId()
            .setJiraProjectIssueConfigurationDetails(
                jiraProjectIssueConfiguration.getJiraProjectIssueConfigurationDetails().toBuilder()
                    .setJiraStatusMappingConfiguration(
                        JiraStatusMappingConfiguration.newBuilder()
                            .addAllStatusMappings(
                                List.of(jiraStatusMappingCommon1, jiraStatusMappingCommon2))
                            .build()))
            .build()
            .getJiraProjectIssueConfigurationDetails(),
        response2.getJiraProjectConfigurations(0).toBuilder()
            .clearConfigurationId()
            .getJiraProjectIssueConfigurationDetails());
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGet")
  void updateProjectIssueConfigurationTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(2, "env2");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    JiraStatusMapping jiraStatusMapping1 =
        CreateJiraStatusMapping(
            "Under Review", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_ISSUES_COMMON_FIXED);
    JiraStatusMapping jiraStatusMapping2 =
        CreateJiraStatusMapping(
            "Open", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_ISSUES_COMMON_UNDER_REVIEW);
    JiraProjectIssueConfiguration jiraProjectIssueConfiguration =
        createDummyJiraProjectIssueConfiguration(
            List.of(jiraStatusMapping1, jiraStatusMapping2), jiraIntegration1.getId());
    jiraAdditionalConfigurationStore.upsertObject(requestContext, jiraProjectIssueConfiguration);

    // Update should fail because config id not found
    UpdateProjectIssueConfigurationRequest request =
        UpdateProjectIssueConfigurationRequest.newBuilder()
            .setConfigurationId("config_id_not_found")
            .setJiraStatusMappingConfiguration(
                JiraStatusMappingConfiguration.newBuilder()
                    .addAllStatusMappings(List.of(jiraStatusMapping1, jiraStatusMapping2))
                    .build())
            .setJiraBidirectionalSyncIsEnabled(true)
            .build();
    assertThrows(StatusRuntimeException.class, () -> stub.updateProjectIssueConfiguration(request));

    // Update should pass with details verification
    UpdateProjectIssueConfigurationRequest request1 =
        UpdateProjectIssueConfigurationRequest.newBuilder()
            .setConfigurationId(jiraProjectIssueConfiguration.getConfigurationId())
            .setJiraStatusMappingConfiguration(
                JiraStatusMappingConfiguration.newBuilder()
                    .addAllStatusMappings(List.of(jiraStatusMapping1, jiraStatusMapping2))
                    .build())
            .setJiraBidirectionalSyncIsEnabled(false)
            .build();
    assertEquals(
        jiraProjectIssueConfiguration.toBuilder()
            .setJiraProjectIssueConfigurationDetails(
                jiraProjectIssueConfiguration.getJiraProjectIssueConfigurationDetails().toBuilder()
                    .setJiraBidirectionalSyncIsEnabled(false)
                    .build())
            .build(),
        stub.updateProjectIssueConfiguration(request1).getJiraProjectConfiguration());
  }

  @Test
  @Tag("useMockUpsertAll")
  @Tag("useMockDelete")
  @Tag("useMockDeleteAll")
  @Tag("useMockGet")
  public void deleteProjectIssueConfigurationTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(2, "env2");
    jiraIntegrationStore.upsertObjects(requestContext, List.of(jiraIntegration1));

    JiraStatusMapping jiraStatusMapping1 =
        CreateJiraStatusMapping(
            "Under Review", TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_ISSUES_COMMON_FIXED);
    JiraProjectIssueConfiguration jiraProjectIssueConfiguration =
        createDummyJiraProjectIssueConfiguration(
            List.of(jiraStatusMapping1), jiraIntegration1.getId());
    JiraProjectIssueConfiguration jiraProjectIssueConfiguration1 =
        createDummyJiraProjectIssueConfiguration(
            List.of(jiraStatusMapping1), jiraIntegration1.getId());
    JiraProjectIssueConfiguration jiraProjectIssueConfiguration2 =
        createDummyJiraProjectIssueConfiguration(
            List.of(jiraStatusMapping1), jiraIntegration1.getId());
    jiraAdditionalConfigurationStore.upsertObjects(
        requestContext,
        List.of(
            jiraProjectIssueConfiguration,
            jiraProjectIssueConfiguration1,
            jiraProjectIssueConfiguration2));

    // Delete should fail if config id not found
    DeleteProjectIssueConfigurationRequest request1 =
        DeleteProjectIssueConfigurationRequest.newBuilder()
            .setConfigurationId("config_id_not_found")
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> stub.deleteProjectIssueConfiguration(request1));

    // Delete should fail if config ids not found
    DeleteProjectIssueConfigurationRequest request2 =
        DeleteProjectIssueConfigurationRequest.newBuilder()
            .addAllConfigurationIds(List.of("config_id_not_found"))
            .build();
    assertThrows(
        StatusRuntimeException.class, () -> stub.deleteProjectIssueConfiguration(request2));

    // Delete should pass if config id found
    DeleteProjectIssueConfigurationRequest request3 =
        DeleteProjectIssueConfigurationRequest.newBuilder()
            .setConfigurationId(jiraProjectIssueConfiguration.getConfigurationId())
            .build();
    assertDoesNotThrow(() -> stub.deleteProjectIssueConfiguration(request3));
    assertFalse(
        jiraAdditionalConfigurationStore
            .getData(requestContext, jiraProjectIssueConfiguration.getConfigurationId())
            .isPresent());

    // Delete should pass if config ids found
    DeleteProjectIssueConfigurationRequest request4 =
        DeleteProjectIssueConfigurationRequest.newBuilder()
            .addAllConfigurationIds(
                List.of(
                    jiraProjectIssueConfiguration1.getConfigurationId(),
                    jiraProjectIssueConfiguration2.getConfigurationId()))
            .build();
    assertDoesNotThrow(() -> stub.deleteProjectIssueConfiguration(request4));
    assertFalse(
        jiraAdditionalConfigurationStore
            .getData(requestContext, jiraProjectIssueConfiguration1.getConfigurationId())
            .isPresent());
    assertFalse(
        jiraAdditionalConfigurationStore
            .getData(requestContext, jiraProjectIssueConfiguration2.getConfigurationId())
            .isPresent());
  }

  private JiraIntegration dummyJiraIntegration(int sr, String... environmentId) {
    JiraIntegration.Builder jiraIntegration =
        JiraIntegration.newBuilder()
            .setId("dummyIntegrationId" + sr)
            .setName("dummyIntegration" + sr)
            .setDescription("dummyDescription")
            .setBaseUrl("dummyLoginUrl")
            .setOverrideBaseUrl("dummyOverrideBaseUrl")
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

  private JiraProjectIssueConfiguration createDummyJiraProjectIssueConfiguration(
      List<JiraStatusMapping> jiraStatusMappingList, String integrationId) {
    return JiraProjectIssueConfiguration.newBuilder()
        .setConfigurationId(CONFIG_ID + Math.random())
        .setJiraProjectIssueConfigurationDetails(
            JiraProjectIssueConfigurationDetails.newBuilder()
                .setJiraBidirectionalSyncIsEnabled(true)
                .setIntegrationId(integrationId)
                .setProjectId(PROJECT_ID)
                .setIssueType(ISSUE_TYPE)
                .setJiraStatusMappingConfiguration(
                    JiraStatusMappingConfiguration.newBuilder()
                        .addAllStatusMappings(jiraStatusMappingList)
                        .build()))
        .build();
  }

  private JiraProjectIssueConfiguration createDummyJiraProjectIssueConfiguration(
      List<JiraStatusMapping> jiraStatusMappingList,
      String integrationId,
      Boolean jiraSyncEnabled) {
    return JiraProjectIssueConfiguration.newBuilder()
        .setConfigurationId(CONFIG_ID + Math.random())
        .setJiraProjectIssueConfigurationDetails(
            JiraProjectIssueConfigurationDetails.newBuilder()
                .setJiraBidirectionalSyncIsEnabled(jiraSyncEnabled)
                .setIntegrationId(integrationId)
                .setProjectId(PROJECT_ID)
                .setIssueType(ISSUE_TYPE)
                .setJiraStatusMappingConfiguration(
                    JiraStatusMappingConfiguration.newBuilder()
                        .addAllStatusMappings(jiraStatusMappingList)
                        .build()))
        .build();
  }

  private JiraStatusMapping CreateJiraStatusMapping(
      String jiraStatus, TraceableEntityStatus traceableEntityStatus) {
    return JiraStatusMapping.newBuilder()
        .setJiraStatus(jiraStatus)
        .setTraceableEntityStatus(traceableEntityStatus)
        .build();
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
        .setOverrideBaseUrl(jiraIntegration.getOverrideBaseUrl())
        .setConsumerKey(jiraIntegration.getConsumerKey())
        .setEncryptedAccessToken(jiraIntegration.getEncryptedAccessToken())
        .setScope(jiraIntegration.getScope())
        .build();
  }
}
