package ai.traceable.jira.integration.config.service;

import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsFilter;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfigurationDetails;
import ai.traceable.jira.integration.config.service.api.v1.JiraTemplate;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityType;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateResponse;
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
                .setIntegrationId(request.getIntegrationId())
                .setIssueType(request.getIssueType())
                .setProjectId(request.getProjectId())
                .addSupportedEntityTypes(request.getSupportedEntityType())
                .build())
            .stream()
            .findFirst()
            .orElseGet(
                () ->
                    this.getDefaultJiraProjectIssueConfiguration(
                        request.getIntegrationId(),
                        request.getIssueType(),
                        request.getProjectId(),
                        request.getSupportedEntityType()))
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
                    .setIntegrationId(updatedJiraTemplate.getIntegrationId())
                    .setIssueType(updatedJiraTemplate.getIssueType())
                    .setProjectId(updatedJiraTemplate.getProjectId())
                    .addSupportedEntityTypes(updatedJiraTemplate.getEntityType())
                    .build())
            .stream()
            .findFirst()
            .map(JiraProjectIssueConfiguration::toBuilder)
            .orElseThrow();

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

    JiraProjectIssueConfiguration.Builder configurationBuilder =
        this.getJiraAdditionalConfiguration(
                requestContext,
                GetProjectIssueConfigurationsFilter.newBuilder()
                    .setIntegrationId(template.getIntegrationId())
                    .setIssueType(template.getIssueType())
                    .setProjectId(template.getProjectId())
                    .addSupportedEntityTypes(template.getEntityType())
                    .build())
            .stream()
            .findFirst()
            .map(JiraProjectIssueConfiguration::toBuilder)
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription(
                            "Jira additional configuration not found for given integrationId, projectId, issueType, entityType")
                        .asRuntimeException());

    List<JiraTemplate> updatedTemplateList =
        configurationBuilder
            .getJiraProjectIssueConfigurationDetails()
            .getJiraTemplateList()
            .stream()
            .filter(jiraTemplate -> !jiraTemplate.getTemplateId().equals(template.getTemplateId()))
            .collect(Collectors.toList());

    this.jiraAdditionalConfigurationStore.upsertObject(
        requestContext,
        configurationBuilder
            .setJiraProjectIssueConfigurationDetails(
                configurationBuilder.getJiraProjectIssueConfigurationDetails().toBuilder()
                    .clearJiraTemplate()
                    .addAllJiraTemplate(updatedTemplateList))
            .build());

    return DeleteJiraTemplateResponse.newBuilder().build();
  }

  List<JiraProjectIssueConfiguration> getJiraAdditionalConfiguration(
      RequestContext requestContext, GetProjectIssueConfigurationsFilter filter) {
    return this.jiraAdditionalConfigurationStore.getAllConfigData(requestContext, filter);
  }

  private JiraProjectIssueConfiguration getDefaultJiraProjectIssueConfiguration(
      String integrationId, String issueType, String projectId, TraceableEntityType entityType) {
    return JiraProjectIssueConfiguration.newBuilder()
        .setConfigurationId(UUID.randomUUID().toString())
        .setJiraProjectIssueConfigurationDetails(
            JiraProjectIssueConfigurationDetails.newBuilder()
                .setIntegrationId(integrationId)
                .setIssueType(issueType)
                .setProjectId(projectId)
                .setValidTraceableEntityType(entityType))
        .build();
  }
}
