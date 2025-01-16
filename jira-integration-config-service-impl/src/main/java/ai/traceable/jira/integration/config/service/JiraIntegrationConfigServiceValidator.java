package ai.traceable.jira.integration.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.EncryptedData;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsFilter;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.JiraCloudAuthCredentials;
import ai.traceable.jira.integration.config.service.api.v1.JiraFieldConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationDetails;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationFilter;
import ai.traceable.jira.integration.config.service.api.v1.JiraStatusMapping;
import ai.traceable.jira.integration.config.service.api.v1.JiraTemplateDetails;
import ai.traceable.jira.integration.config.service.api.v1.Scope;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityType;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateProjectIssueConfigurationRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class JiraIntegrationConfigServiceValidator {
  private final JiraIntegrationStore jiraIntegrationStore;

  public void validateCreateJiraIntegration(
      CreateJiraIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateJiraIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, CreateJiraIntegrationRequest.BASE_URL_FIELD_NUMBER);
    if (request.hasJiraIntegrationDetails()) {
      validateJiraIntegrationDetailsOrThrow(request.getJiraIntegrationDetails());
    } else {
      validateNonDefaultPresenceOrThrow(
          request, CreateJiraIntegrationRequest.CONSUMER_KEY_FIELD_NUMBER);
      validateEncryptedDataOrThrow(request.getEncryptedAccessToken());
    }
    validateUniqueNameOrThrow(requestContext, request.getName());
    if (request.hasScope()) {
      validateScopeForMutationOrThrow(request.getScope(), requestContext, Collections.emptySet());
    } else {
      validateUnscopedForMutationOrThrow(requestContext, Collections.emptySet());
    }
  }

  public void validateAddJiraTemplate(
      AddJiraTemplateRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, AddJiraTemplateRequest.INTEGRATION_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, AddJiraTemplateRequest.ISSUE_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, AddJiraTemplateRequest.PROJECT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, AddJiraTemplateRequest.SUPPORTED_ENTITY_TYPE_FIELD_NUMBER);
    this.validateJiraTemplateDetails(request.getJiraTemplateDetails());
  }

  public void validateUpdateJiraTemplate(
      UpdateJiraTemplateRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateJiraTemplateRequest.TEMPLATE_ID_FIELD_NUMBER);
    this.validateJiraTemplateDetails(request.getJiraTemplateDetails());
  }

  public void validateDeleteJiraTemplate(
      DeleteJiraTemplateRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteJiraTemplateRequest.TEMPLATE_ID_FIELD_NUMBER);
  }

  public void validateCreateProjectIssueConfiguration(
      CreateProjectIssueConfigurationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, CreateProjectIssueConfigurationRequest.INTEGRATION_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, CreateProjectIssueConfigurationRequest.ISSUE_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, CreateProjectIssueConfigurationRequest.PROJECT_ID_FIELD_NUMBER);
    request
        .getJiraStatusMappingConfiguration()
        .getStatusMappingsList()
        .forEach(this::validateJiraStatusMapping);
    if (!request.getFieldConfigurationsList().isEmpty())
      request.getFieldConfigurationsList().forEach(this::validateJiraFieldConfiguration);
  }

  public void validateUpdateProjectIssueConfiguration(
      UpdateProjectIssueConfigurationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateProjectIssueConfigurationRequest.CONFIGURATION_ID_FIELD_NUMBER);
    request
        .getJiraStatusMappingConfiguration()
        .getStatusMappingsList()
        .forEach(this::validateJiraStatusMapping);
    request.getFieldConfigurationsList().forEach(this::validateJiraFieldConfiguration);
  }

  public void validateDeleteProjectIssueConfiguration(
      DeleteProjectIssueConfigurationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    if (!request.getConfigurationId().isEmpty()) {
      validateNonDefaultPresenceOrThrow(
          request, DeleteProjectIssueConfigurationRequest.CONFIGURATION_ID_FIELD_NUMBER);
    } else {
      validateNonDefaultPresenceOrThrow(
          request, DeleteProjectIssueConfigurationRequest.CONFIGURATION_IDS_FIELD_NUMBER);
    }
  }

  public void validateGetProjectIssueConfiguration(
      GetProjectIssueConfigurationsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    this.validateGetProjectIssueConfigurationsFilter(request.getFilter());
  }

  private void validateGetProjectIssueConfigurationsFilter(
      GetProjectIssueConfigurationsFilter filter) {
    if (filter.hasIntegrationId()) {
      validateNonDefaultPresenceOrThrow(
          filter, GetProjectIssueConfigurationsFilter.INTEGRATION_ID_FIELD_NUMBER);
    }
    if (filter.hasProjectId()) {
      validateNonDefaultPresenceOrThrow(
          filter, GetProjectIssueConfigurationsFilter.PROJECT_ID_FIELD_NUMBER);
    }
    if (filter.hasIssueType()) {
      validateNonDefaultPresenceOrThrow(
          filter, GetProjectIssueConfigurationsFilter.ISSUE_TYPE_FIELD_NUMBER);
    }
    if (filter.getSupportedEntityTypesList().stream()
        .anyMatch(
            entityType ->
                (entityType == TraceableEntityType.UNRECOGNIZED
                    || entityType == TraceableEntityType.TRACEABLE_ENTITY_TYPE_UNSPECIFIED))) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Supported entity types should be valid")
          .asRuntimeException();
    }
  }

  private void validateJiraStatusMapping(JiraStatusMapping statusMapping) {
    validateNonDefaultPresenceOrThrow(statusMapping, JiraStatusMapping.JIRA_STATUS_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        statusMapping, JiraStatusMapping.TRACEABLE_ENTITY_STATUS_FIELD_NUMBER);
  }

  private void validateJiraFieldConfiguration(JiraFieldConfiguration jiraFieldConfiguration) {
    validateNonDefaultPresenceOrThrow(
        jiraFieldConfiguration, JiraFieldConfiguration.FIELD_KEY_FIELD_NUMBER);
    if (jiraFieldConfiguration.hasOverriddenDynamicValue()) {
      validateNonDefaultPresenceOrThrow(
          jiraFieldConfiguration, JiraFieldConfiguration.OVERRIDDEN_DYNAMIC_VALUE_FIELD_NUMBER);
    }
    if (jiraFieldConfiguration.hasOverriddenDefaultValueJsonString()) {
      validateNonDefaultPresenceOrThrow(
          jiraFieldConfiguration,
          JiraFieldConfiguration.OVERRIDDEN_DEFAULT_VALUE_JSON_STRING_FIELD_NUMBER);
    }
  }

  private void validateJiraTemplateDetails(JiraTemplateDetails details) {
    validateNonDefaultPresenceOrThrow(details, JiraTemplateDetails.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        details, JiraTemplateDetails.MARKDOWN_FORMAT_VALUE_FIELD_NUMBER);
  }

  private void validateJiraIntegrationDetailsOrThrow(
      JiraIntegrationDetails jiraIntegrationDetails) {
    if (jiraIntegrationDetails.hasJiraDataCenterIntegrationDetails()) {
      validateEncryptedDataOrThrow(
          jiraIntegrationDetails
              .getJiraDataCenterIntegrationDetails()
              .getJiraDataCenterAuthCredentials()
              .getEncryptedPersonalAccessToken());
    } else if (jiraIntegrationDetails.hasJiraCloudIntegrationDetails()) {
      validateNonDefaultPresenceOrThrow(
          jiraIntegrationDetails.getJiraCloudIntegrationDetails().getJiraCloudAuthCredentials(),
          JiraCloudAuthCredentials.CONSUMER_KEY_FIELD_NUMBER);
      validateEncryptedDataOrThrow(
          jiraIntegrationDetails
              .getJiraCloudIntegrationDetails()
              .getJiraCloudAuthCredentials()
              .getEncryptedAccessToken());
    }
  }

  private void validateUnscopedForMutationOrThrow(
      RequestContext requestContext, Set<String> allowedJiraIntegrationIds) {
    if (containsOtherJiraIntegrations(
        jiraIntegrationStore.getAllConfigData(requestContext), allowedJiraIntegrationIds)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Cannot persist an unscoped integration as it will conflict with one or more existing integrations.")
          .asRuntimeException();
    }
  }

  private void validateScopeForMutationOrThrow(
      Scope scope, RequestContext requestContext, Set<String> allowedJiraIntegrationIds) {
    validateScopeOrThrow(scope);
    JiraIntegrationFilter filter = JiraIntegrationFilter.newBuilder().setFilterScope(scope).build();
    if (containsOtherJiraIntegrations(
        jiraIntegrationStore.getAllConfigData(requestContext, filter), allowedJiraIntegrationIds)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Given scope is not suitable for mutation as it conflicts with existing integrations.")
          .asRuntimeException();
    }
  }

  private boolean containsOtherJiraIntegrations(
      List<JiraIntegration> jiraIntegrationList, Set<String> allowedJiraIntegrationIds) {
    return !allowedJiraIntegrationIds.containsAll(
        jiraIntegrationList.stream()
            .map(JiraIntegration::getId)
            .collect(Collectors.toUnmodifiableList()));
  }

  private void validateEncryptedDataOrThrow(EncryptedData encryptedData) {
    validateNonDefaultPresenceOrThrow(encryptedData, EncryptedData.KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        encryptedData, EncryptedData.BASE64_ENCRYPTED_VALUE_FIELD_NUMBER);
  }

  public void validateGetJiraIntegrations(
      GetJiraIntegrationsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateJiraIntegrationFilterOrThrow(request.getJiraIntegrationFilter());
  }

  private void validateJiraIntegrationFilterOrThrow(JiraIntegrationFilter filter) {
    if (filter.hasFilterScope()) {
      validateScopeOrThrow(filter.getFilterScope());
    }
  }

  private void validateScopeOrThrow(Scope scope) {
    switch (scope.getScopeTypeCase()) {
      case ENVIRONMENT_IDS:
        if (scope.getEnvironmentIds().getValuesList().isEmpty()) {
          throw Status.INVALID_ARGUMENT
              .withDescription("Environment-ids in Scope cannot be an empty list")
              .asRuntimeException();
        }
        break;
      case SCOPETYPE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT.withDescription("Invalid Scope").asRuntimeException();
    }
  }

  public void validateUpdateJiraIntegration(
      UpdateJiraIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateJiraIntegrationRequest.JIRA_INTEGRATION_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateJiraIntegrationRequest.NAME_FIELD_NUMBER);
    if (request.hasScope()) {
      validateScopeForMutationOrThrow(
          request.getScope(), requestContext, Set.of(request.getJiraIntegrationId()));
    } else {
      validateUnscopedForMutationOrThrow(requestContext, Set.of(request.getJiraIntegrationId()));
    }
    validateUniqueNameOrThrow(requestContext, request.getName(), request.getJiraIntegrationId());
  }

  public void validateDeleteJiraIntegration(
      DeleteJiraIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteJiraIntegrationRequest.JIRA_INTEGRATION_ID_FIELD_NUMBER);
  }

  private void validateUniqueNameOrThrow(RequestContext requestContext, String name) {
    if (jiraIntegrationStore.getAllConfigData(requestContext).stream()
        .anyMatch(integration -> integration.getName().equals(name))) {
      throw getDuplicateNameStatusRuntimeException();
    }
  }

  private void validateUniqueNameOrThrow(
      RequestContext requestContext, String name, String jiraIntegrationId) {
    if (jiraIntegrationStore.getAllConfigData(requestContext).stream()
        .filter(integration -> !jiraIntegrationId.equals(integration.getId()))
        .anyMatch(integration -> integration.getName().equals(name))) {
      throw getDuplicateNameStatusRuntimeException();
    }
  }

  private StatusRuntimeException getDuplicateNameStatusRuntimeException() {
    return Status.INVALID_ARGUMENT
        .withDescription("Already an existing jira integration with the same name")
        .asRuntimeException();
  }
}
