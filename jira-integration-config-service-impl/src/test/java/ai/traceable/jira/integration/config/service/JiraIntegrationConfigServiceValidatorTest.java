package ai.traceable.jira.integration.config.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DynamicField;
import ai.traceable.jira.integration.config.service.api.v1.EncryptedData;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsFilter;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.JiraCloudAuthCredentials;
import ai.traceable.jira.integration.config.service.api.v1.JiraCloudIntegrationDetails;
import ai.traceable.jira.integration.config.service.api.v1.JiraFieldConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationDetails;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationFilter;
import ai.traceable.jira.integration.config.service.api.v1.JiraStatusMapping;
import ai.traceable.jira.integration.config.service.api.v1.JiraStatusMappingConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.Scope;
import ai.traceable.jira.integration.config.service.api.v1.StringList;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityStatus;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityType;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateProjectIssueConfigurationRequest;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JiraIntegrationConfigServiceValidatorTest {
  @Mock JiraIntegrationStore jiraIntegrationStore;
  @InjectMocks JiraIntegrationConfigServiceValidator validator;
  private final String TENANT_ID = "tenant-id";
  private final String TEXT = "text";
  private final String PROJECT_ID = "project_id";
  private final String ISSUE_TYPE = "issue_type";
  private final String CONFIG_ID = "config_id";
  private final String INTEGRATION_ID = "integration_id";

  @Test
  void validateCreateJiraIntegration() {
    CreateJiraIntegrationRequest createJiraIntegrationRequest =
        CreateJiraIntegrationRequest.newBuilder().build();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateJiraIntegration(createJiraIntegrationRequest, requestContext));

    // Should throw Runtime Exception (no fields declared)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    CreateJiraIntegrationRequest createRequest =
        CreateJiraIntegrationRequest.newBuilder(createJiraIntegrationRequest).build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest, requestContext2));

    // Should throw Runtime Exception (no encrypted-access-token)
    CreateJiraIntegrationRequest createRequest1 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest).toBuilder()
            .clearEncryptedAccessToken()
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest1, requestContext2));

    when(jiraIntegrationStore.getAllConfigData(requestContext2)).thenReturn(List.of());

    // should pass with all required fields declared
    CreateJiraIntegrationRequest createRequest2 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest);
    Assertions.assertDoesNotThrow(
        () -> validator.validateCreateJiraIntegration(createRequest2, requestContext2));

    // should fail for scope without at least one environment
    CreateJiraIntegrationRequest createRequest3 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest).toBuilder()
            .setScope(Scope.newBuilder().setEnvironmentIds(StringList.getDefaultInstance()))
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest3, requestContext2));

    // should pass with all required fields declared and valid environment
    CreateJiraIntegrationRequest createRequest4 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest).toBuilder()
            .setScope(Scope.newBuilder().setEnvironmentIds(StringList.newBuilder().addValues(TEXT)))
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateCreateJiraIntegration(createRequest4, requestContext2));

    // should throw because an environment can be fetched with the set scope
    when(jiraIntegrationStore.getAllConfigData(
            requestContext2,
            JiraIntegrationFilter.newBuilder().setFilterScope(createRequest4.getScope()).build()))
        .thenReturn(List.of(JiraIntegration.getDefaultInstance()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest4, requestContext2));

    // should fail for scope of invalid type
    CreateJiraIntegrationRequest createRequest5 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest).toBuilder()
            .setScope(Scope.newBuilder().build())
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest5, requestContext2));

    // should pass with valid jiraIntegrationDetials and no population of deprecated consumerKey and
    // encryptedAccessToken
    CreateJiraIntegrationRequest createRequest6 =
        createRequest4.toBuilder()
            .setOverrideBaseUrl(TEXT)
            .clearEncryptedAccessToken()
            .clearConsumerKey()
            .clearScope()
            .setJiraIntegrationDetails(
                JiraIntegrationDetails.newBuilder()
                    .setJiraCloudIntegrationDetails(
                        JiraCloudIntegrationDetails.newBuilder()
                            .setJiraCloudAuthCredentials(
                                JiraCloudAuthCredentials.newBuilder()
                                    .setConsumerKey(TEXT)
                                    .setEncryptedAccessToken(
                                        EncryptedData.newBuilder()
                                            .setBase64EncryptedValue(TEXT)
                                            .setKeyId(TEXT)))))
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateCreateJiraIntegration(createRequest6, requestContext2));

    // should fail due to existing integration with the set scope even with valid
    // jiraIntegrationDetials and no population of deprecated consumerKey and encryptedAccessToken
    CreateJiraIntegrationRequest createRequest7 =
        createRequest6.toBuilder().setScope(createRequest4.getScope()).build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest7, requestContext2));

    // should fail due to non unique naming of integration
    when(jiraIntegrationStore.getAllConfigData(requestContext2))
        .thenReturn(List.of(JiraIntegration.newBuilder().setName(TEXT).build()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest6, requestContext2));
  }

  private CreateJiraIntegrationRequest minimalCreateJiraIntegrationRequest(
      CreateJiraIntegrationRequest createJiraIntegrationRequest) {
    return CreateJiraIntegrationRequest.newBuilder(createJiraIntegrationRequest)
        .setConsumerKey(TEXT)
        .setBaseUrl(TEXT)
        .setName(TEXT)
        .setEncryptedAccessToken(
            EncryptedData.newBuilder().setBase64EncryptedValue(TEXT).setKeyId(TEXT))
        .build();
  }

  @Test
  void validateGetJiraIntegration() {
    GetJiraIntegrationsRequest getJiraIntegrationsRequest =
        GetJiraIntegrationsRequest.getDefaultInstance();
    // Should throw runtime Exception (no TenantId)
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateGetJiraIntegrations(getJiraIntegrationsRequest, requestContext));

    // Should not throw runtime Exception (default empty filter)
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertDoesNotThrow(
        () -> validator.validateGetJiraIntegrations(getJiraIntegrationsRequest, requestContext1));

    // Should not throw runtime Exception (filter with valid environment)
    GetJiraIntegrationsRequest getJiraIntegrationsRequest1 =
        getJiraIntegrationsRequest.toBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues(TEXT))))
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateGetJiraIntegrations(getJiraIntegrationsRequest1, requestContext1));
  }

  @Test
  void validateUpdateJiraIntegration() {
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest =
        UpdateJiraIntegrationRequest.newBuilder().build();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateJiraIntegration(updateJiraIntegrationRequest, requestContext));

    // Should throw Runtime Exception (no integrationId)
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateJiraIntegration(updateJiraIntegrationRequest, requestContext1));

    // Should throw Runtime Exception (missing name)
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest1 =
        updateJiraIntegrationRequest.toBuilder().setJiraIntegrationId(TEXT).build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateJiraIntegration(
                updateJiraIntegrationRequest1, requestContext1));

    when(jiraIntegrationStore.getAllConfigData(
            requestContext1,
            JiraIntegrationFilter.newBuilder()
                .setFilterScope(
                    Scope.newBuilder().setEnvironmentIds(StringList.newBuilder().addValues(TEXT)))
                .build()))
        .thenReturn(
            List.of(
                JiraIntegration.newBuilder()
                    .setId(updateJiraIntegrationRequest1.getJiraIntegrationId())
                    .setName(TEXT)
                    .build()));

    // should pass with integrationId and any one of name, update-scope present
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest2 =
        updateJiraIntegrationRequest1.toBuilder()
            .setName(TEXT)
            .setOverrideBaseUrl(TEXT)
            .setScope(Scope.newBuilder().setEnvironmentIds(StringList.newBuilder().addValues(TEXT)))
            .build();
    when(jiraIntegrationStore.getAllConfigData(requestContext1))
        .thenReturn(
            List.of(
                JiraIntegration.newBuilder()
                    .setId(updateJiraIntegrationRequest2.getJiraIntegrationId())
                    .setName(TEXT)
                    .setOverrideBaseUrl(TEXT)
                    .build()));
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateUpdateJiraIntegration(
                updateJiraIntegrationRequest2, requestContext1));

    // will fail because the name in update request already exists
    when(jiraIntegrationStore.getAllConfigData(requestContext1))
        .thenReturn(
            List.of(
                JiraIntegration.newBuilder().setId("otherIntegrationId").setName(TEXT).build()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateJiraIntegration(
                updateJiraIntegrationRequest2, requestContext1));
  }

  @Test
  void validateDeleteJiraIntegration() {
    DeleteJiraIntegrationRequest deleteJiraIntegrationRequest =
        DeleteJiraIntegrationRequest.newBuilder().build();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateDeleteJiraIntegration(deleteJiraIntegrationRequest, requestContext));

    // Should throw Runtime Exception (no integrationId)
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateDeleteJiraIntegration(deleteJiraIntegrationRequest, requestContext1));
  }

  @Test
  void validateCreateProjectIssueConfiguration() {
    CreateProjectIssueConfigurationRequest request =
        CreateProjectIssueConfigurationRequest.newBuilder().build();

    // Should throw runtime exception, no tenant id
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateProjectIssueConfiguration(request, requestContext));

    // Should throw runtime exception, fields not declared
    RequestContext requestContext1 = RequestContext.forTenantId("tenant_id");
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateProjectIssueConfiguration(request, requestContext1));

    // Should pass without field configuration
    JiraStatusMapping jiraStatusMapping1 =
        CreateJiraStatusMapping(
            "jira_status_1",
            TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_AST_VULNERABILITY_ACCEPTED_RISK);
    JiraStatusMapping jiraStatusMapping2 =
        CreateJiraStatusMapping(
            "jira_status_2",
            TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_AST_VULNERABILITY_NOT_AN_ISSUE);
    CreateProjectIssueConfigurationRequest request1 =
        request.toBuilder()
            .setIntegrationId(INTEGRATION_ID)
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setJiraStatusMappingConfiguration(
                JiraStatusMappingConfiguration.newBuilder()
                    .addAllStatusMappings(List.of(jiraStatusMapping1, jiraStatusMapping2))
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateCreateProjectIssueConfiguration(request1, requestContext1));

    // Should pass with valid field configuration
    JiraFieldConfiguration jiraFieldConfiguration =
        JiraFieldConfiguration.newBuilder()
            .setFieldKey("field1")
            .setOverriddenDefaultValueJsonString("override_value")
            .setOverriddenDynamicValueValue(DynamicField.DYNAMIC_FIELD_LOGGED_IN_USER_VALUE)
            .build();
    CreateProjectIssueConfigurationRequest request2 =
        request1.toBuilder().addAllFieldConfigurations(List.of(jiraFieldConfiguration)).build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateCreateProjectIssueConfiguration(request2, requestContext1));
  }

  @Test
  void validateUpdateProjectIssueConfiguration() {
    UpdateProjectIssueConfigurationRequest request =
        UpdateProjectIssueConfigurationRequest.newBuilder().build();

    // Should throw runtime exception, no tenant id
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateUpdateProjectIssueConfiguration(request, requestContext));

    // Should pass if all required fields set
    RequestContext requestContext1 = RequestContext.forTenantId("tenant_id");
    JiraStatusMapping jiraStatusMapping1 =
        CreateJiraStatusMapping(
            "jira_status_1",
            TraceableEntityStatus.TRACEABLE_ENTITY_STATUS_AST_VULNERABILITY_ACCEPTED_RISK);
    UpdateProjectIssueConfigurationRequest request1 =
        request.toBuilder()
            .setConfigurationId(CONFIG_ID)
            .setJiraBidirectionalSyncIsEnabled(true)
            .setJiraStatusMappingConfiguration(
                JiraStatusMappingConfiguration.newBuilder()
                    .addAllStatusMappings(List.of(jiraStatusMapping1))
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateUpdateProjectIssueConfiguration(request1, requestContext1));
  }

  @Test
  void validateGetProjectIssueConfiguration() {
    GetProjectIssueConfigurationsRequest request =
        GetProjectIssueConfigurationsRequest.newBuilder().build();

    // Should throw runtime exception, no tenant id
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateGetProjectIssueConfiguration(request, requestContext));

    // Should Pass with required fields set except TraceableEntityType
    RequestContext requestContext1 = RequestContext.forTenantId("tenant_id");
    GetProjectIssueConfigurationsRequest request1 =
        request.toBuilder()
            .setFilter(
                GetProjectIssueConfigurationsFilter.newBuilder()
                    .setIntegrationId(INTEGRATION_ID)
                    .setProjectId(PROJECT_ID)
                    .setIssueType(ISSUE_TYPE)
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateGetProjectIssueConfiguration(request1, requestContext1));

    // Should Pass with all required fields set
    GetProjectIssueConfigurationsRequest request2 =
        request1.toBuilder()
            .setFilter(
                request1.getFilter().toBuilder()
                    .addAllSupportedEntityTypes(
                        List.of(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY))
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateGetProjectIssueConfiguration(request2, requestContext1));
  }

  @Test
  void validateDeleteProjectIssueConfiguration() {
    DeleteProjectIssueConfigurationRequest request =
        DeleteProjectIssueConfigurationRequest.newBuilder().build();

    // Should throw runtime exception, no tenant id
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateDeleteProjectIssueConfiguration(request, requestContext));

    RequestContext requestContext1 = RequestContext.forTenantId("tenant_id");

    // Delete should fail if config id or config ids not set
    assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateDeleteProjectIssueConfiguration(request, requestContext));

    // Should pass with config id set
    DeleteProjectIssueConfigurationRequest request1 =
        request.toBuilder().setConfigurationId(CONFIG_ID).build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateDeleteProjectIssueConfiguration(request1, requestContext1));

    // Should pass with config ids set
    DeleteProjectIssueConfigurationRequest request2 =
        request.toBuilder().addAllConfigurationIds(List.of(CONFIG_ID)).build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateDeleteProjectIssueConfiguration(request2, requestContext1));
  }

  @Test
  void validateAddJiraTemplate() {
    AddJiraTemplateRequest request = AddJiraTemplateRequest.newBuilder().build();

    // Should throw runtime exception, no tenant id
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateAddJiraTemplate(request, requestContext));

    // Should throw runtime exception, fields not declared
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateAddJiraTemplate(request, requestContext1));

    // Should throw runtime exception, missing integration_id
    AddJiraTemplateRequest request1 =
        request.toBuilder()
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateAddJiraTemplate(request1, requestContext1));

    // Should throw runtime exception, missing project_id
    AddJiraTemplateRequest request2 =
        request.toBuilder()
            .setIntegrationId(INTEGRATION_ID)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateAddJiraTemplate(request2, requestContext1));

    // Should throw runtime exception, missing issue_type
    AddJiraTemplateRequest request3 =
        request.toBuilder()
            .setIntegrationId(INTEGRATION_ID)
            .setProjectId(PROJECT_ID)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateAddJiraTemplate(request3, requestContext1));

    // Should throw runtime exception, missing supported_entity_type
    AddJiraTemplateRequest request4 =
        request.toBuilder()
            .setIntegrationId(INTEGRATION_ID)
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateAddJiraTemplate(request4, requestContext1));

    // Should pass with all required fields set
    AddJiraTemplateRequest request5 =
        request.toBuilder()
            .setIntegrationId(INTEGRATION_ID)
            .setProjectId(PROJECT_ID)
            .setIssueType(ISSUE_TYPE)
            .setSupportedEntityType(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
            .setJiraTemplateDetails(
                ai.traceable.jira.integration.config.service.api.v1.JiraTemplateDetails.newBuilder()
                    .setName("Template Name")
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateAddJiraTemplate(request5, requestContext1));
  }

  @Test
  void validateUpdateJiraTemplate() {
    UpdateJiraTemplateRequest request = UpdateJiraTemplateRequest.newBuilder().build();

    // Should throw runtime exception, no tenant id
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateUpdateJiraTemplate(request, requestContext));

    // Should throw runtime exception, missing template_id
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateUpdateJiraTemplate(request, requestContext1));

    // Should pass with template_id set
    UpdateJiraTemplateRequest request1 =
        request.toBuilder()
            .setTemplateId("template_id")
            .setJiraTemplateDetails(
                ai.traceable.jira.integration.config.service.api.v1.JiraTemplateDetails.newBuilder()
                    .setName("Updated Template Name")
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateUpdateJiraTemplate(request1, requestContext1));
  }

  @Test
  void validateDeleteJiraTemplate() {
    DeleteJiraTemplateRequest request = DeleteJiraTemplateRequest.newBuilder().build();

    // Should throw runtime exception, no tenant id
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateDeleteJiraTemplate(request, requestContext));

    // Should throw runtime exception, missing template_id
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateDeleteJiraTemplate(request, requestContext1));

    // Should pass with template_id set
    DeleteJiraTemplateRequest request1 = request.toBuilder().setTemplateId("template_id").build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateDeleteJiraTemplate(request1, requestContext1));
  }

  @Test
  void validateGetJiraTemplates() {
    GetJiraTemplatesRequest request = GetJiraTemplatesRequest.newBuilder().build();

    // Should throw runtime exception, no tenant id
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateGetJiraTemplates(request, requestContext));

    // Should pass with no filter (default request)
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertDoesNotThrow(
        () -> validator.validateGetJiraTemplates(request, requestContext1));

    // Should pass with valid entity types filter
    GetJiraTemplatesRequest request1 =
        request.toBuilder()
            .setFilter(
                ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesFilter
                    .newBuilder()
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY)
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_THREAT_ACTIVITY)
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateGetJiraTemplates(request1, requestContext1));

    // Should throw runtime exception with invalid entity type (UNSPECIFIED)
    GetJiraTemplatesRequest request2 =
        request.toBuilder()
            .setFilter(
                ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesFilter
                    .newBuilder()
                    .addEntityTypes(TraceableEntityType.TRACEABLE_ENTITY_TYPE_UNSPECIFIED)
                    .build())
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateGetJiraTemplates(request2, requestContext1));

    // Should pass with template_id filter
    GetJiraTemplatesRequest request3 =
        request.toBuilder()
            .setFilter(
                ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesFilter
                    .newBuilder()
                    .setTemplateId("template_id")
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateGetJiraTemplates(request3, requestContext1));
  }

  private JiraStatusMapping CreateJiraStatusMapping(
      String jiraStatus, TraceableEntityStatus traceableEntityStatus) {
    return JiraStatusMapping.newBuilder()
        .setJiraStatus(jiraStatus)
        .setTraceableEntityStatus(traceableEntityStatus)
        .build();
  }
}
