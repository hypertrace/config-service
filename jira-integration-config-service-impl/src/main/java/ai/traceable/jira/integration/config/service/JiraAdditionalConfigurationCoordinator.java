package ai.traceable.jira.integration.config.service;

import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.CreateProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateProjectIssueConfigurationResponse;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.DeleteProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteProjectIssueConfigurationResponse;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesFilter;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraTemplatesResponse;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsFilter;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfigurationDetails;
import ai.traceable.jira.integration.config.service.api.v1.JiraTemplate;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityType;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.UpdateProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateProjectIssueConfigurationResponse;
import ai.traceable.jira.integration.config.service.util.JiraAdditionalConfigStatusMappingUtil;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
class JiraAdditionalConfigurationCoordinator {
  private final JiraAdditionalConfigurationStore jiraAdditionalConfigurationStore;

  AddJiraTemplateResponse addJiraTemplate(
      AddJiraTemplateRequest request, RequestContext requestContext) {
    // For a given integrationId, issueType, projectId, entityType there would be at-most one
    // configuration, hence we are fetching the first one
    JiraProjectIssueConfiguration.Builder configurationBuilder =
        this.getJiraAdditionalConfiguration(
            requestContext,
            GetProjectIssueConfigurationsFilter.newBuilder()
                .addIntegrationIds(request.getIntegrationId())
                .setIssueType(request.getIssueType())
                .setProjectId(request.getProjectId())
                .build())
            .stream()
            .findFirst()
            .orElseGet(
                () ->
                    this.getDefaultJiraProjectIssueConfiguration(
                        request.getIntegrationId(), request.getIssueType(), request.getProjectId()))
            .toBuilder();

    JiraTemplate jiraTemplate =
        JiraTemplate.newBuilder()
            .setTemplateId(UUID.randomUUID().toString())
            .setIntegrationId(request.getIntegrationId())
            .setProjectId(request.getProjectId())
            .setIssueType(request.getIssueType())
            .setEntityType(request.getSupportedEntityType())
            .setJiraTemplateDetails(request.getJiraTemplateDetails())
            .build();

    this.jiraAdditionalConfigurationStore.upsertObject(
        requestContext,
        configurationBuilder
            .setJiraProjectIssueConfigurationDetails(
                configurationBuilder.getJiraProjectIssueConfigurationDetails().toBuilder()
                    .addJiraTemplate(jiraTemplate))
            .build());

    return AddJiraTemplateResponse.newBuilder().setJiraTemplate(jiraTemplate).build();
  }

  UpdateJiraTemplateResponse updateJiraTemplate(
      UpdateJiraTemplateRequest request, RequestContext requestContext) {
    JiraTemplate existingJiraTemplate =
        this.jiraAdditionalConfigurationStore
            .getJiraTemplate(requestContext, request.getTemplateId())
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription("Jira Template not found for id")
                        .asRuntimeException());

    JiraTemplate updatedJiraTemplate =
        existingJiraTemplate.toBuilder()
            .setJiraTemplateDetails(request.getJiraTemplateDetails())
            .build();

    JiraProjectIssueConfiguration.Builder configurationBuilder =
        this.getJiraAdditionalConfiguration(
                requestContext,
                GetProjectIssueConfigurationsFilter.newBuilder()
                    .addIntegrationIds(updatedJiraTemplate.getIntegrationId())
                    .setIssueType(updatedJiraTemplate.getIssueType())
                    .setProjectId(updatedJiraTemplate.getProjectId())
                    .build())
            .stream()
            .findFirst()
            .map(JiraProjectIssueConfiguration::toBuilder)
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription(
                            String.format(
                                "Jira configuration not found for integrationId: %s, projectId: %s, issueType: %s",
                                updatedJiraTemplate.getIntegrationId(),
                                updatedJiraTemplate.getProjectId(),
                                updatedJiraTemplate.getIssueType()))
                        .asRuntimeException());
    List<JiraTemplate> updatedTemplateList =
        configurationBuilder
            .getJiraProjectIssueConfigurationDetails()
            .getJiraTemplateList()
            .stream()
            .map(
                jiraTemplate ->
                    jiraTemplate.getTemplateId().equals(updatedJiraTemplate.getTemplateId())
                        ? updatedJiraTemplate
                        : jiraTemplate)
            .collect(Collectors.toList());

    this.jiraAdditionalConfigurationStore.upsertObject(
        requestContext,
        configurationBuilder
            .setJiraProjectIssueConfigurationDetails(
                configurationBuilder.getJiraProjectIssueConfigurationDetails().toBuilder()
                    .clearJiraTemplate()
                    .addAllJiraTemplate(updatedTemplateList))
            .build());

    return UpdateJiraTemplateResponse.newBuilder().setJiraTemplate(updatedJiraTemplate).build();
  }

  DeleteJiraTemplateResponse deleteJiraTemplate(
      DeleteJiraTemplateRequest request, RequestContext requestContext) {
    JiraTemplate template =
        this.jiraAdditionalConfigurationStore
            .getJiraTemplate(requestContext, request.getTemplateId())
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription("Jira Template not found for id")
                        .asRuntimeException());

    JiraProjectIssueConfiguration configuration =
        this.getJiraAdditionalConfiguration(
                requestContext,
                GetProjectIssueConfigurationsFilter.newBuilder()
                    .addIntegrationIds(template.getIntegrationId())
                    .setIssueType(template.getIssueType())
                    .setProjectId(template.getProjectId())
                    .build())
            .stream()
            .findFirst()
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription(
                            String.format(
                                "Jira configuration not found for integrationId: %s, projectId: %s, issueType: %s",
                                template.getIntegrationId(),
                                template.getProjectId(),
                                template.getIssueType()))
                        .asRuntimeException());

    List<JiraTemplate> updatedTemplateList =
        configuration.getJiraProjectIssueConfigurationDetails().getJiraTemplateList().stream()
            .filter(jiraTemplate -> !jiraTemplate.getTemplateId().equals(template.getTemplateId()))
            .collect(Collectors.toList());

    // Check if we should delete the entire document or just update it
    boolean hasStatusMappings =
        configuration.getJiraProjectIssueConfigurationDetails().hasJiraStatusMappingConfiguration()
            && !configuration
                .getJiraProjectIssueConfigurationDetails()
                .getJiraStatusMappingConfiguration()
                .getStatusMappingsList()
                .isEmpty();

    boolean hasFieldConfigurations =
        !configuration
            .getJiraProjectIssueConfigurationDetails()
            .getFieldConfigurationsList()
            .isEmpty();

    // Delete entire document if templates will be empty and no status mappings or field
    // configurations exist
    if (updatedTemplateList.isEmpty() && !hasStatusMappings && !hasFieldConfigurations) {
      this.jiraAdditionalConfigurationStore.deleteObject(
          requestContext, configuration.getConfigurationId());
    } else {
      // Otherwise, just update with the remaining templates
      this.jiraAdditionalConfigurationStore.upsertObject(
          requestContext,
          configuration.toBuilder()
              .setJiraProjectIssueConfigurationDetails(
                  configuration.getJiraProjectIssueConfigurationDetails().toBuilder()
                      .clearJiraTemplate()
                      .addAllJiraTemplate(updatedTemplateList))
              .build());
    }

    return DeleteJiraTemplateResponse.newBuilder().build();
  }

  GetJiraTemplatesResponse getJiraTemplates(
      GetJiraTemplatesRequest request, RequestContext requestContext) {

    String templateId = null;
    List<TraceableEntityType> entityTypes = null;
    if (request.hasFilter()) {
      GetJiraTemplatesFilter filter = request.getFilter();
      templateId = filter.hasTemplateId() ? filter.getTemplateId() : null;
      entityTypes = !filter.getEntityTypesList().isEmpty() ? filter.getEntityTypesList() : null;
    }

    List<JiraTemplate> templates =
        this.jiraAdditionalConfigurationStore.getJiraTemplates(
            requestContext, templateId, entityTypes);

    return GetJiraTemplatesResponse.newBuilder().addAllJiraTemplates(templates).build();
  }

  CreateProjectIssueConfigurationResponse createProjectIssueConfiguration(
      CreateProjectIssueConfigurationRequest request, RequestContext requestContext) {

    if (this.getJiraAdditionalConfiguration(
            requestContext,
            GetProjectIssueConfigurationsFilter.newBuilder()
                .addIntegrationIds(request.getIntegrationId())
                .setIssueType(request.getIssueType())
                .setProjectId(request.getProjectId())
                .build())
        .stream()
        .findFirst()
        .isPresent()) {
      throw Status.ALREADY_EXISTS
          .withDescription("Configuration already exists")
          .asRuntimeException();
    }

    JiraProjectIssueConfiguration configuration =
        JiraProjectIssueConfiguration.newBuilder()
            .setConfigurationId(UUID.randomUUID().toString())
            .setJiraProjectIssueConfigurationDetails(
                JiraProjectIssueConfigurationDetails.newBuilder()
                    .setIntegrationId(request.getIntegrationId())
                    .setIssueType(request.getIssueType())
                    .setProjectId(request.getProjectId())
                    .setJiraStatusMappingConfiguration(request.getJiraStatusMappingConfiguration())
                    .addAllFieldConfigurations(request.getFieldConfigurationsList())
                    .setJiraBidirectionalSyncIsEnabled(request.getJiraBidirectionalSyncIsEnabled()))
            .build();

    this.jiraAdditionalConfigurationStore.upsertObject(requestContext, configuration);

    return CreateProjectIssueConfigurationResponse.newBuilder()
        .setJiraProjectConfiguration(configuration)
        .build();
  }

  UpdateProjectIssueConfigurationResponse updateProjectIssueConfiguration(
      UpdateProjectIssueConfigurationRequest request, RequestContext requestContext) {

    JiraProjectIssueConfiguration jiraProjectIssueConfiguration =
        this.jiraAdditionalConfigurationStore
            .getData(requestContext, request.getConfigurationId())
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription("Configuration not found")
                        .asRuntimeException());

    JiraProjectIssueConfiguration updatedConfiguration =
        jiraProjectIssueConfiguration.toBuilder()
            .setJiraProjectIssueConfigurationDetails(
                jiraProjectIssueConfiguration.getJiraProjectIssueConfigurationDetails().toBuilder()
                    .setJiraStatusMappingConfiguration(request.getJiraStatusMappingConfiguration())
                    .clearFieldConfigurations()
                    .addAllFieldConfigurations(request.getFieldConfigurationsList())
                    .setJiraBidirectionalSyncIsEnabled(request.getJiraBidirectionalSyncIsEnabled()))
            .build();

    this.jiraAdditionalConfigurationStore.upsertObject(requestContext, updatedConfiguration);

    return UpdateProjectIssueConfigurationResponse.newBuilder()
        .setJiraProjectConfiguration(updatedConfiguration)
        .build();
  }

  DeleteProjectIssueConfigurationResponse deleteProjectIssueConfiguration(
      DeleteProjectIssueConfigurationRequest request, RequestContext requestContext) {
    if (!request.getConfigurationId().isEmpty()) {
      this.jiraAdditionalConfigurationStore
          .deleteObject(requestContext, request.getConfigurationId())
          .orElseThrow(
              () ->
                  Status.NOT_FOUND
                      .withDescription(
                          "Configuration not found for ID: " + request.getConfigurationId())
                      .asRuntimeException());
    } else {
      try {
        this.jiraAdditionalConfigurationStore.deleteObjects(
            requestContext, request.getConfigurationIdsList());
      } catch (Exception e) {
        throw Status.NOT_FOUND
            .withDescription("One or more configurations not found")
            .withCause(e)
            .asRuntimeException();
      }
    }
    return DeleteProjectIssueConfigurationResponse.getDefaultInstance();
  }

  public List<JiraProjectIssueConfiguration> getJiraAdditionalConfiguration(
      RequestContext requestContext, GetProjectIssueConfigurationsFilter filter) {
    // Get all configurations
    List<JiraProjectIssueConfiguration> allConfigurations =
        this.jiraAdditionalConfigurationStore.getAllConfigData(
            requestContext, filter.toBuilder().clearSupportedEntityTypes().build());
    // if no entity type provided, return all mappings
    if (filter.getSupportedEntityTypesList().isEmpty()) {
      return allConfigurations;
    }
    return JiraAdditionalConfigStatusMappingUtil.getFilteredConfiguration(
        filter, allConfigurations);
  }

  private JiraProjectIssueConfiguration getDefaultJiraProjectIssueConfiguration(
      String integrationId, String issueType, String projectId) {
    return JiraProjectIssueConfiguration.newBuilder()
        .setConfigurationId(UUID.randomUUID().toString())
        .setJiraProjectIssueConfigurationDetails(
            JiraProjectIssueConfigurationDetails.newBuilder()
                .setIntegrationId(integrationId)
                .setIssueType(issueType)
                .setProjectId(projectId))
        .build();
  }
}
