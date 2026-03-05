package ai.traceable.jira.integration.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.EncryptedData;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsResponse;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesFilter;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesResponse;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsFilter;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsResponse;
import ai.traceable.jira.integration.config.service.api.v1.JiraFieldTemplate;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationConfigServiceGrpc;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationConfigServiceGrpc.JiraIntegrationConfigServiceBlockingStub;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationFilter;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfigurationDetails;
import ai.traceable.jira.integration.config.service.api.v1.JiraStatusMapping;
import ai.traceable.jira.integration.config.service.api.v1.JiraStatusMappingConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.JiraTemplate;
import ai.traceable.jira.integration.config.service.api.v1.JiraTemplateDetails;
import ai.traceable.jira.integration.config.service.api.v1.Scope;
import ai.traceable.jira.integration.config.service.api.v1.StaticFieldValue;
import ai.traceable.jira.integration.config.service.api.v1.StringList;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityStatus;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityType;
import ai.traceable.jira.integration.config.service.api.v1.TraceableField;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateRequest;
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
  private final String PROJECT_ID_2 = "project2";
  private final String ISSUE_TYPE = "issue_type";
  private final String ISSUE_TYPE_BUG = "bug";
  private final String CONFIG_ID = "config_id";
  private static final String TEMPLATE_NAME_VULNERABILITY = "Vulnerability Template";
  private static final String TEMPLATE_NAME_AST_VULNERABILITY = "AST Vulnerability Template";
  private static final String TEMPLATE_NAME_THREAT_ACTIVITY = "Threat Activity Template";
  private static final String TEMPLATE_NAME_ORIGINAL = "Original Template";
  private static final String TEMPLATE_NAME_UPDATED = "Updated Template";
  private static final String TEMPLATE_NAME_TO_DELETE = "Template to Delete";
  private static final String MARKDOWN_VULNERABILITY_DETAILS = "## Vulnerability Details";
  private static final String MARKDOWN_ORIGINAL_CONTENT = "## Original Content";
  private static final String MARKDOWN_UPDATED_CONTENT = "## Updated Content";
  private static final String JIRA_FIELD_KEY_1 = "summary";
  private static final String JIRA_FIELD_KEY_2 = "severity";
  private static final String STATIC_VALUE_1 = "Critical";
  private static final String STATIC_VALUE_2 = "High";
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
                new JiraIntegrationConfigServiceValidator(
                    jiraIntegrationStore, jiraAdditionalConfigurationStore),
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
            .setOverrideBaseUrl("newOverrideBaseUrl")
            .build();
    JiraIntegration expectedUpdatedJiraIntegration =
        jiraIntegration1.toBuilder()
            .setName(updateJiraIntegrationRequest1.getName())
            .setDescription(updateJiraIntegrationRequest1.getDescription())
            .setScope(updateJiraIntegrationRequest1.getScope())
            .setOverrideBaseUrl(updateJiraIntegrationRequest1.getOverrideBaseUrl())
            .build();
    assertDoesNotThrow(() -> stub.updateJiraIntegration(updateJiraIntegrationRequest1));
    assertEquals(
        expectedUpdatedJiraIntegration,
        jiraIntegrationStore.getData(requestContext, jiraIntegration1.getId()).orElseThrow());

    // this should successfully unset the scope and override base url
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest2 =
        updateJiraIntegrationRequest1.toBuilder().clearScope().clearOverrideBaseUrl().build();
    stub.updateJiraIntegration(updateJiraIntegrationRequest2);
    assertFalse(
        jiraIntegrationStore
            .getData(requestContext, jiraIntegration1.getId())
            .orElseThrow()
            .hasScope());
    assertFalse(
        jiraIntegrationStore
            .getData(requestContext, jiraIntegration1.getId())
            .orElseThrow()
            .hasOverrideBaseUrl());

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
  @Tag("useMockDeleteAll")
  @Tag("useMockGetAll")
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

    //
    JiraIntegration jiraIntegration2 = dummyJiraIntegration(2, "env2");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration2);
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
            jiraIntegration2.getId());
    JiraProjectIssueConfiguration jiraProjectIssueConfiguration1 =
        createDummyJiraProjectIssueConfiguration(
            List.of(
                jiraStatusMappingCommon1,
                jiraStatusMappingCommon2,
                jiraStatusMappingSpecificEntityType1,
                jiraStatusMappingSpecificEntityType2),
            jiraIntegration2.getId());
    jiraAdditionalConfigurationStore.upsertObject(requestContext, jiraProjectIssueConfiguration);
    jiraAdditionalConfigurationStore.upsertObject(requestContext, jiraProjectIssueConfiguration1);
    DeleteJiraIntegrationRequest deleteJiraIntegrationRequest2 =
        DeleteJiraIntegrationRequest.newBuilder()
            .setJiraIntegrationId(jiraIntegration2.getId())
            .build();
    assertDoesNotThrow(() -> stub.deleteJiraIntegration(deleteJiraIntegrationRequest2));
    assertFalse(
        jiraAdditionalConfigurationStore
            .getData(requestContext, jiraIntegration2.getId())
            .isPresent());
    assertFalse(jiraIntegrationStore.getData(requestContext, jiraIntegration2.getId()).isPresent());
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

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void addJiraTemplateTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    // Should succeed in adding a template
    JiraFieldTemplate fieldTemplate1 =
        JiraFieldTemplate.newBuilder()
            .setFieldKey(JIRA_FIELD_KEY_1)
            .setIsEnabled(true)
            .setStaticValue(StaticFieldValue.newBuilder().setFieldValue(STATIC_VALUE_1).build())
            .build();
    JiraFieldTemplate fieldTemplate2 =
        JiraFieldTemplate.newBuilder()
            .setFieldKey(JIRA_FIELD_KEY_2)
            .setIsEnabled(true)
            .setTraceableField(TraceableField.TRACEABLE_FIELD_VULNERABILITY_SEVERITY)
            .build();
    AddJiraTemplateRequest addRequest =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName(TEMPLATE_NAME_VULNERABILITY)
                    .setMarkdownFormatValue(MARKDOWN_VULNERABILITY_DETAILS)
                    .addFieldTemplates(fieldTemplate1)
                    .addFieldTemplates(fieldTemplate2)
                    .build())
            .build();
    JiraTemplate createdTemplate =
        assertDoesNotThrow(() -> stub.addJiraTemplate(addRequest).getJiraTemplate());
    assertEquals(jiraIntegration1.getId(), createdTemplate.getIntegrationId());
    assertEquals(PROJECT_ID, createdTemplate.getProjectId());
    assertEquals(ISSUE_TYPE, createdTemplate.getIssueType());
    assertEquals(
        TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY,
        createdTemplate.getEntityType());
    assertEquals(TEMPLATE_NAME_VULNERABILITY, createdTemplate.getJiraTemplateDetails().getName());
    assertEquals(2, createdTemplate.getJiraTemplateDetails().getFieldTemplatesCount());
    assertEquals(
        JIRA_FIELD_KEY_1,
        createdTemplate.getJiraTemplateDetails().getFieldTemplates(0).getFieldKey());
    assertTrue(createdTemplate.getJiraTemplateDetails().getFieldTemplates(0).hasStaticValue());
    assertEquals(
        JIRA_FIELD_KEY_2,
        createdTemplate.getJiraTemplateDetails().getFieldTemplates(1).getFieldKey());
    assertTrue(createdTemplate.getJiraTemplateDetails().getFieldTemplates(1).hasTraceableField());
    assertEquals(
        TraceableField.TRACEABLE_FIELD_VULNERABILITY_SEVERITY,
        createdTemplate.getJiraTemplateDetails().getFieldTemplates(1).getTraceableField());

    // Should fail with invalid entity type
    AddJiraTemplateRequest invalidRequest =
        addRequest.toBuilder()
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_UNSPECIFIED)
            .build();
    assertThrows(StatusRuntimeException.class, () -> stub.addJiraTemplate(invalidRequest));
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void updateJiraTemplateTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    // Add a template first
    JiraFieldTemplate originalFieldTemplate =
        JiraFieldTemplate.newBuilder()
            .setFieldKey(JIRA_FIELD_KEY_1)
            .setIsEnabled(true)
            .setStaticValue(StaticFieldValue.newBuilder().setFieldValue(STATIC_VALUE_1).build())
            .build();
    AddJiraTemplateRequest addRequest =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName(TEMPLATE_NAME_ORIGINAL)
                    .setMarkdownFormatValue(MARKDOWN_ORIGINAL_CONTENT)
                    .addFieldTemplates(originalFieldTemplate)
                    .build())
            .build();
    JiraTemplate createdTemplate = stub.addJiraTemplate(addRequest).getJiraTemplate();

    // Should fail with non-existent template id
    UpdateJiraTemplateRequest updateRequestWrong =
        UpdateJiraTemplateRequest.newBuilder()
            .setTemplateId("non_existent_id")
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder().setName("Updated Template").build())
            .build();
    assertThrows(StatusRuntimeException.class, () -> stub.updateJiraTemplate(updateRequestWrong));

    // Should succeed with valid template id
    JiraFieldTemplate updatedFieldTemplate1 =
        JiraFieldTemplate.newBuilder()
            .setFieldKey(JIRA_FIELD_KEY_1)
            .setIsEnabled(true)
            .setStaticValue(StaticFieldValue.newBuilder().setFieldValue(STATIC_VALUE_2).build())
            .build();
    JiraFieldTemplate updatedFieldTemplate2 =
        JiraFieldTemplate.newBuilder()
            .setFieldKey(JIRA_FIELD_KEY_2)
            .setIsEnabled(false)
            .setTraceableField(TraceableField.TRACEABLE_FIELD_VULNERABILITY_CWE)
            .build();
    UpdateJiraTemplateRequest updateRequest =
        UpdateJiraTemplateRequest.newBuilder()
            .setTemplateId(createdTemplate.getTemplateId())
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName(TEMPLATE_NAME_UPDATED)
                    .setMarkdownFormatValue(MARKDOWN_UPDATED_CONTENT)
                    .addFieldTemplates(updatedFieldTemplate1)
                    .addFieldTemplates(updatedFieldTemplate2)
                    .build())
            .build();
    JiraTemplate updatedTemplate =
        assertDoesNotThrow(() -> stub.updateJiraTemplate(updateRequest).getJiraTemplate());
    assertEquals(createdTemplate.getTemplateId(), updatedTemplate.getTemplateId());
    assertEquals(TEMPLATE_NAME_UPDATED, updatedTemplate.getJiraTemplateDetails().getName());
    assertEquals(
        MARKDOWN_UPDATED_CONTENT,
        updatedTemplate.getJiraTemplateDetails().getMarkdownFormatValue());
    assertEquals(2, updatedTemplate.getJiraTemplateDetails().getFieldTemplatesCount());
    assertEquals(
        JIRA_FIELD_KEY_1,
        updatedTemplate.getJiraTemplateDetails().getFieldTemplates(0).getFieldKey());
    assertEquals(
        STATIC_VALUE_2,
        updatedTemplate
            .getJiraTemplateDetails()
            .getFieldTemplates(0)
            .getStaticValue()
            .getFieldValue());
    assertEquals(
        JIRA_FIELD_KEY_2,
        updatedTemplate.getJiraTemplateDetails().getFieldTemplates(1).getFieldKey());
    assertFalse(updatedTemplate.getJiraTemplateDetails().getFieldTemplates(1).getIsEnabled());
    assertEquals(
        TraceableField.TRACEABLE_FIELD_VULNERABILITY_CWE,
        updatedTemplate.getJiraTemplateDetails().getFieldTemplates(1).getTraceableField());
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  @Tag("useMockDelete")
  void deleteJiraTemplateTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    // Add a template first
    JiraFieldTemplate deleteFieldTemplate =
        JiraFieldTemplate.newBuilder()
            .setFieldKey(JIRA_FIELD_KEY_1)
            .setIsEnabled(true)
            .setStaticValue(StaticFieldValue.newBuilder().setFieldValue(STATIC_VALUE_1).build())
            .build();
    AddJiraTemplateRequest addRequest =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName(TEMPLATE_NAME_TO_DELETE)
                    .addFieldTemplates(deleteFieldTemplate)
                    .build())
            .build();
    JiraTemplate createdTemplate = stub.addJiraTemplate(addRequest).getJiraTemplate();

    // Should fail with non-existent template id
    DeleteJiraTemplateRequest deleteRequestWrong =
        DeleteJiraTemplateRequest.newBuilder().setTemplateId("non_existent_id").build();
    assertThrows(StatusRuntimeException.class, () -> stub.deleteJiraTemplate(deleteRequestWrong));

    // Should succeed with valid template id
    DeleteJiraTemplateRequest deleteRequest =
        DeleteJiraTemplateRequest.newBuilder()
            .setTemplateId(createdTemplate.getTemplateId())
            .build();
    assertDoesNotThrow(() -> stub.deleteJiraTemplate(deleteRequest));

    // Verify template is deleted by trying to get it
    GetJiraTemplatesRequest getRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .setTemplateId(createdTemplate.getTemplateId())
                    .build())
            .build();
    GetJiraTemplatesResponse getResponse = stub.getJiraTemplates(getRequest);
    assertEquals(0, getResponse.getJiraTemplatesCount());
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void getJiraTemplatesTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    // Add multiple templates with different entity types
    JiraFieldTemplate astVulnFieldTemplate =
        JiraFieldTemplate.newBuilder()
            .setFieldKey(JIRA_FIELD_KEY_1)
            .setIsEnabled(true)
            .setTraceableField(TraceableField.TRACEABLE_FIELD_VULNERABILITY_SEVERITY)
            .build();
    AddJiraTemplateRequest addRequest1 =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName(TEMPLATE_NAME_AST_VULNERABILITY)
                    .addFieldTemplates(astVulnFieldTemplate)
                    .build())
            .build();
    JiraTemplate template1 = stub.addJiraTemplate(addRequest1).getJiraTemplate();

    JiraFieldTemplate threatActivityFieldTemplate =
        JiraFieldTemplate.newBuilder()
            .setFieldKey(JIRA_FIELD_KEY_2)
            .setIsEnabled(true)
            .setTraceableField(TraceableField.TRACEABLE_FIELD_THREAT_ACTIVITY_SEVERITY)
            .build();
    AddJiraTemplateRequest addRequest2 =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE_BUG)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_THREAT_ACTIVITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName(TEMPLATE_NAME_THREAT_ACTIVITY)
                    .addFieldTemplates(threatActivityFieldTemplate)
                    .build())
            .build();
    JiraTemplate template2 = stub.addJiraTemplate(addRequest2).getJiraTemplate();

    JiraFieldTemplate vulnFieldTemplate =
        JiraFieldTemplate.newBuilder()
            .setFieldKey(JIRA_FIELD_KEY_2)
            .setIsEnabled(true)
            .setTraceableField(TraceableField.TRACEABLE_FIELD_VULNERABILITY_CWE)
            .build();
    AddJiraTemplateRequest addRequest3 =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID_2)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName(TEMPLATE_NAME_VULNERABILITY)
                    .addFieldTemplates(vulnFieldTemplate)
                    .build())
            .build();
    JiraTemplate template3 = stub.addJiraTemplate(addRequest3).getJiraTemplate();

    // Get all templates (no filter)
    GetJiraTemplatesRequest getAllRequest = GetJiraTemplatesRequest.newBuilder().build();
    GetJiraTemplatesResponse getAllResponse = stub.getJiraTemplates(getAllRequest);
    assertEquals(3, getAllResponse.getJiraTemplatesCount());

    // Get templates by specific template id
    GetJiraTemplatesRequest getByIdRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .setTemplateId(template1.getTemplateId())
                    .build())
            .build();
    GetJiraTemplatesResponse getByIdResponse = stub.getJiraTemplates(getByIdRequest);
    assertEquals(1, getByIdResponse.getJiraTemplatesCount());
    assertEquals(template1.getTemplateId(), getByIdResponse.getJiraTemplates(0).getTemplateId());
    assertEquals(
        1, getByIdResponse.getJiraTemplates(0).getJiraTemplateDetails().getFieldTemplatesCount());
    assertEquals(
        JIRA_FIELD_KEY_1,
        getByIdResponse
            .getJiraTemplates(0)
            .getJiraTemplateDetails()
            .getFieldTemplates(0)
            .getFieldKey());

    // Get templates by entity type - THREAT_ACTIVITY (no mapping involved)
    GetJiraTemplatesRequest getByEntityTypeRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_THREAT_ACTIVITY)
                    .build())
            .build();
    GetJiraTemplatesResponse getByEntityTypeResponse =
        stub.getJiraTemplates(getByEntityTypeRequest);
    assertEquals(1, getByEntityTypeResponse.getJiraTemplatesCount());
    assertEquals(
        TraceableEntityType.TRACEABLE_ENTITY_TYPE_THREAT_ACTIVITY,
        getByEntityTypeResponse.getJiraTemplates(0).getEntityType());
    assertEquals(
        1,
        getByEntityTypeResponse
            .getJiraTemplates(0)
            .getJiraTemplateDetails()
            .getFieldTemplatesCount());
    assertEquals(
        TraceableField.TRACEABLE_FIELD_THREAT_ACTIVITY_SEVERITY,
        getByEntityTypeResponse
            .getJiraTemplates(0)
            .getJiraTemplateDetails()
            .getFieldTemplates(0)
            .getTraceableField());

    // Get templates by multiple entity types
    // AST_VULNERABILITY maps to VULNERABILITY, so this should return all 3 templates
    GetJiraTemplatesRequest getByMultipleEntityTypesRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_THREAT_ACTIVITY)
                    .build())
            .build();
    GetJiraTemplatesResponse getByMultipleEntityTypesResponse =
        stub.getJiraTemplates(getByMultipleEntityTypesRequest);
    // Should return all 3: template1 (AST_VULNERABILITY), template2 (THREAT_ACTIVITY), template3
    // (VULNERABILITY)
    assertEquals(3, getByMultipleEntityTypesResponse.getJiraTemplatesCount());

    // Test AST_VULNERABILITY mapping: requesting AST_VULNERABILITY should also return VULNERABILITY
    // templates
    GetJiraTemplatesRequest getAstVulnMappingRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
                    .build())
            .build();
    GetJiraTemplatesResponse getAstVulnMappingResponse =
        stub.getJiraTemplates(getAstVulnMappingRequest);
    assertEquals(2, getAstVulnMappingResponse.getJiraTemplatesCount());
    assertTrue(
        getAstVulnMappingResponse.getJiraTemplatesList().stream()
            .anyMatch(t -> t.getTemplateId().equals(template1.getTemplateId())));
    assertTrue(
        getAstVulnMappingResponse.getJiraTemplatesList().stream()
            .anyMatch(t -> t.getTemplateId().equals(template3.getTemplateId())));

    GetJiraTemplatesRequest getVulnRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY)
                    .build())
            .build();
    GetJiraTemplatesResponse getVulnResponse = stub.getJiraTemplates(getVulnRequest);
    assertEquals(2, getVulnResponse.getJiraTemplatesCount());
    assertTrue(
        getVulnResponse.getJiraTemplatesList().stream()
            .anyMatch(t -> t.getTemplateId().equals(template1.getTemplateId())));
    assertTrue(
        getVulnResponse.getJiraTemplatesList().stream()
            .anyMatch(t -> t.getTemplateId().equals(template3.getTemplateId())));

    GetJiraTemplatesRequest getBothVulnTypesRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY)
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
                    .build())
            .build();
    GetJiraTemplatesResponse getBothVulnTypesResponse =
        stub.getJiraTemplates(getBothVulnTypesRequest);
    assertEquals(2, getBothVulnTypesResponse.getJiraTemplatesCount());
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void getJiraTemplatesByPrefixTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    // Add templates with different name prefixes
    AddJiraTemplateRequest addRequest1 =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName("Vulnerability Template")
                    .setMarkdownFormatValue(MARKDOWN_VULNERABILITY_DETAILS)
                    .build())
            .build();
    JiraTemplate template1 = stub.addJiraTemplate(addRequest1).getJiraTemplate();

    AddJiraTemplateRequest addRequest2 =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE_BUG)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName("Vulnerability Report")
                    .setMarkdownFormatValue("## AST Vulnerability")
                    .build())
            .build();
    JiraTemplate template2 = stub.addJiraTemplate(addRequest2).getJiraTemplate();

    AddJiraTemplateRequest addRequest3 =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID_2)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_THREAT_ACTIVITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName("Threat Activity Template")
                    .setMarkdownFormatValue("## Threat Activity")
                    .build())
            .build();
    JiraTemplate template3 = stub.addJiraTemplate(addRequest3).getJiraTemplate();

    // Get all templates (no filter)
    GetJiraTemplatesRequest getAllRequest = GetJiraTemplatesRequest.newBuilder().build();
    GetJiraTemplatesResponse getAllResponse = stub.getJiraTemplates(getAllRequest);
    assertEquals(3, getAllResponse.getJiraTemplatesCount());

    // Get templates by prefix "Vulnerability"
    GetJiraTemplatesRequest getByPrefixRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(GetJiraTemplatesFilter.newBuilder().setPrefix("Vulnerability").build())
            .build();
    GetJiraTemplatesResponse getByPrefixResponse = stub.getJiraTemplates(getByPrefixRequest);
    assertEquals(2, getByPrefixResponse.getJiraTemplatesCount());
    assertTrue(
        getByPrefixResponse.getJiraTemplatesList().stream()
            .allMatch(t -> t.getJiraTemplateDetails().getName().startsWith("Vulnerability")));

    // Get templates by prefix "Threat"
    GetJiraTemplatesRequest getByThreatPrefixRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(GetJiraTemplatesFilter.newBuilder().setPrefix("Threat").build())
            .build();
    GetJiraTemplatesResponse getByThreatPrefixResponse =
        stub.getJiraTemplates(getByThreatPrefixRequest);
    assertEquals(1, getByThreatPrefixResponse.getJiraTemplatesCount());
    assertEquals(
        "Threat Activity Template",
        getByThreatPrefixResponse.getJiraTemplates(0).getJiraTemplateDetails().getName());

    // Get templates by non-matching prefix
    GetJiraTemplatesRequest getNonMatchingPrefixRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(GetJiraTemplatesFilter.newBuilder().setPrefix("NonExistent").build())
            .build();
    GetJiraTemplatesResponse getNonMatchingPrefixResponse =
        stub.getJiraTemplates(getNonMatchingPrefixRequest);
    assertEquals(0, getNonMatchingPrefixResponse.getJiraTemplatesCount());

    // Get templates by prefix combined with entity type filter
    // Since VULNERABILITY and AST_VULNERABILITY are merged, this should return both templates
    GetJiraTemplatesRequest getCombinedFilterRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .setPrefix("Vulnerability")
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY)
                    .build())
            .build();
    GetJiraTemplatesResponse getCombinedFilterResponse =
        stub.getJiraTemplates(getCombinedFilterRequest);
    assertEquals(2, getCombinedFilterResponse.getJiraTemplatesCount());
    assertTrue(
        getCombinedFilterResponse.getJiraTemplatesList().stream()
            .allMatch(t -> t.getJiraTemplateDetails().getName().startsWith("Vulnerability")));
    assertTrue(
        getCombinedFilterResponse.getJiraTemplatesList().stream()
            .anyMatch(t -> t.getTemplateId().equals(template1.getTemplateId())));
    assertTrue(
        getCombinedFilterResponse.getJiraTemplatesList().stream()
            .anyMatch(t -> t.getTemplateId().equals(template2.getTemplateId())));
  }

  @Test
  @Tag("useMockUpsert")
  @Tag("useMockGetAll")
  void getJiraTemplatesWithAstVulnerabilityMappingTest() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    JiraIntegration jiraIntegration1 = dummyJiraIntegration(1, "env1");
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration1);

    // Add a template with VULNERABILITY entity type
    AddJiraTemplateRequest addRequest =
        AddJiraTemplateRequest.newBuilder()
            .setIntegrationId(jiraIntegration1.getId())
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY)
            .setJiraTemplateDetails(
                JiraTemplateDetails.newBuilder()
                    .setName(TEMPLATE_NAME_VULNERABILITY)
                    .setMarkdownFormatValue(MARKDOWN_VULNERABILITY_DETAILS)
                    .build())
            .build();
    JiraTemplate vulnerabilityTemplate = stub.addJiraTemplate(addRequest).getJiraTemplate();

    // Request templates with AST_VULNERABILITY entity type
    GetJiraTemplatesRequest getByAstVulnRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
                    .build())
            .build();
    GetJiraTemplatesResponse getByAstVulnResponse = stub.getJiraTemplates(getByAstVulnRequest);

    // Should return the VULNERABILITY template
    assertEquals(1, getByAstVulnResponse.getJiraTemplatesCount());
    assertEquals(
        vulnerabilityTemplate.getTemplateId(),
        getByAstVulnResponse.getJiraTemplates(0).getTemplateId());
    assertEquals(
        TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY,
        getByAstVulnResponse.getJiraTemplates(0).getEntityType());

    // Request with both VULNERABILITY and AST_VULNERABILITY should return same template once
    GetJiraTemplatesRequest getBothTypesRequest =
        GetJiraTemplatesRequest.newBuilder()
            .setFilter(
                GetJiraTemplatesFilter.newBuilder()
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY)
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
                    .build())
            .build();
    GetJiraTemplatesResponse getBothTypesResponse = stub.getJiraTemplates(getBothTypesRequest);

    // Should still return only 1 template (not duplicated)
    assertEquals(1, getBothTypesResponse.getJiraTemplatesCount());
    assertEquals(
        vulnerabilityTemplate.getTemplateId(),
        getBothTypesResponse.getJiraTemplates(0).getTemplateId());
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
