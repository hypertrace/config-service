package ai.traceable.jira.integration.config.service;

import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.AddJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationResponse;
import ai.traceable.jira.integration.config.service.api.v1.CreateProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateProjectIssueConfigurationResponse;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.DeleteProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteProjectIssueConfigurationResponse;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsFilter;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsResponse;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationFilter;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraTemplateResponse;
import ai.traceable.jira.integration.config.service.api.v1.UpdateProjectIssueConfigurationRequest;
import ai.traceable.jira.integration.config.service.api.v1.UpdateProjectIssueConfigurationResponse;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
public class JiraIntegrationCoordinator {
  private final JiraIntegrationStore jiraIntegrationStore;
  private final JiraAdditionalConfigurationCoordinator jiraAdditionalConfigurationCoordinator;
  private final JiraAdditionalConfigurationStore jiraAdditionalConfigurationStore;

  public CreateJiraIntegrationResponse createJiraIntegration(
      CreateJiraIntegrationRequest request, RequestContext requestContext) {
    // populating the deprecated fields for backward compatibility
    JiraIntegration.Builder jiraIntegration =
        JiraIntegration.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setName(request.getName())
            .setConsumerKey(request.getConsumerKey())
            .setBaseUrl(request.getBaseUrl())
            .setEncryptedAccessToken(request.getEncryptedAccessToken());
    if (request.hasScope()) {
      jiraIntegration.setScope(request.getScope());
    }
    if (request.hasDescription()) {
      jiraIntegration.setDescription(request.getDescription());
    }
    if (request.hasJiraIntegrationDetails()) {
      jiraIntegration.setJiraIntegrationDetails(request.getJiraIntegrationDetails());
    }
    if (request.hasOverrideBaseUrl()) {
      jiraIntegration.setOverrideBaseUrl(request.getOverrideBaseUrl());
    }
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration.build());
    return CreateJiraIntegrationResponse.newBuilder().setIntegration(jiraIntegration).build();
  }

  public List<JiraIntegration> getJiraIntegration(
      RequestContext requestContext, GetJiraIntegrationsRequest request) {
    List<JiraIntegration> jiraIntegrations;
    if (request.hasJiraIntegrationFilter()) {
      jiraIntegrations =
          jiraIntegrationStore.getAllConfigData(requestContext, request.getJiraIntegrationFilter());
    } else {
      jiraIntegrations = jiraIntegrationStore.getAllConfigData(requestContext);
    }
    // early return if no integrations exist
    if (jiraIntegrations.isEmpty()) {
      return jiraIntegrations;
    }
    List<JiraProjectIssueConfiguration> allProjectIssueConfigurations =
        jiraAdditionalConfigurationStore.getAllConfigData(requestContext);
    Map<String, Boolean> integrationIdToSyncFlag =
        allProjectIssueConfigurations.stream()
            .collect(
                Collectors.toMap(
                    config -> config.getJiraProjectIssueConfigurationDetails().getIntegrationId(),
                    config ->
                        config
                            .getJiraProjectIssueConfigurationDetails()
                            .getJiraBidirectionalSyncIsEnabled(),
                    Boolean::logicalOr));
    return jiraIntegrations.stream()
        .map(
            integration -> {
              boolean biDirectionalSyncEnabled =
                  integrationIdToSyncFlag.getOrDefault(integration.getId(), false);
              return integration.toBuilder()
                  .setJiraBidirectionalSyncIsEnabled(biDirectionalSyncEnabled)
                  .build();
            })
        .collect(Collectors.toList());
  }

  public JiraIntegration updateJiraIntegration(
      UpdateJiraIntegrationRequest request, RequestContext requestContext) {
    JiraIntegration.Builder jiraIntegrationAtBuilder =
        jiraIntegrationStore
            .getData(requestContext, request.getJiraIntegrationId())
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription(
                            "Unable to update Jira-Integration with given Id as it does not exist")
                        .asRuntimeException())
            .toBuilder();

    jiraIntegrationAtBuilder.setDescription(request.getDescription());
    jiraIntegrationAtBuilder.setName(request.getName());
    jiraIntegrationAtBuilder.clearScope();
    if (request.hasScope()) {
      jiraIntegrationAtBuilder.setScope(request.getScope());
    }
    if (request.hasOverrideBaseUrl()) {
      jiraIntegrationAtBuilder.setOverrideBaseUrl(request.getOverrideBaseUrl());
    } else {
      jiraIntegrationAtBuilder.clearOverrideBaseUrl();
    }
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegrationAtBuilder.build());
    return jiraIntegrationAtBuilder.build();
  }

  public void deleteIntegration(
      RequestContext requestContext, DeleteJiraIntegrationRequest request) {
    jiraIntegrationStore
        .deleteObject(requestContext, request.getJiraIntegrationId())
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        "Unable to delete Jira-Integration with given Id as it does not exist")
                    .asRuntimeException());
  }

  public AddJiraTemplateResponse addJiraTemplate(
      RequestContext requestContext, AddJiraTemplateRequest request) {
    return this.jiraAdditionalConfigurationCoordinator.addJiraTemplate(request, requestContext);
  }

  public UpdateJiraTemplateResponse updateJiraTemplate(
      RequestContext requestContext, UpdateJiraTemplateRequest request) {
    return this.jiraAdditionalConfigurationCoordinator.updateJiraTemplate(request, requestContext);
  }

  public DeleteJiraTemplateResponse deleteJiraTemplate(
      RequestContext requestContext, DeleteJiraTemplateRequest request) {
    return this.jiraAdditionalConfigurationCoordinator.deleteJiraTemplate(request, requestContext);
  }

  public CreateProjectIssueConfigurationResponse createProjectIssueConfiguration(
      RequestContext requestContext, CreateProjectIssueConfigurationRequest request) {
    return this.jiraAdditionalConfigurationCoordinator.createProjectIssueConfiguration(
        request, requestContext);
  }

  public UpdateProjectIssueConfigurationResponse updateProjectIssueConfiguration(
      RequestContext requestContext, UpdateProjectIssueConfigurationRequest request) {
    return this.jiraAdditionalConfigurationCoordinator.updateProjectIssueConfiguration(
        request, requestContext);
  }

  public DeleteProjectIssueConfigurationResponse deleteProjectIssueConfiguration(
      RequestContext requestContext, DeleteProjectIssueConfigurationRequest request) {
    return this.jiraAdditionalConfigurationCoordinator.deleteProjectIssueConfiguration(
        request, requestContext);
  }

  public GetProjectIssueConfigurationsResponse getProjectIssueConfigurations(
      RequestContext requestContext, GetProjectIssueConfigurationsRequest request) {
    List<JiraProjectIssueConfiguration> issueConfigurations =
        normalizeFilter(requestContext, request.getFilter())
            .map(
                normalizedFilter ->
                    this.jiraAdditionalConfigurationCoordinator.getJiraAdditionalConfiguration(
                        requestContext, normalizedFilter))
            .orElse(List.of());
    return GetProjectIssueConfigurationsResponse.newBuilder()
        .addAllJiraProjectConfigurations(issueConfigurations)
        .build();
  }

  private Optional<GetProjectIssueConfigurationsFilter> normalizeFilter(
      RequestContext requestContext, GetProjectIssueConfigurationsFilter filter) {
    Set<Set<String>> idRestrictions = new HashSet<>();

    if (filter.hasIntegrationId()) {
      idRestrictions.add(Set.of(filter.getIntegrationId()));
    }

    if (!filter.getIntegrationIdsList().isEmpty()) {
      idRestrictions.add(Set.copyOf(filter.getIntegrationIdsList()));
    }

    if (filter.hasScope()) {
      Set<String> integrationIdsInScope =
          this.jiraIntegrationStore
              .getAllConfigData(
                  requestContext,
                  JiraIntegrationFilter.newBuilder().setFilterScope(filter.getScope()).build())
              .stream()
              .map(JiraIntegration::getId)
              .collect(Collectors.toUnmodifiableSet());
      idRestrictions.add(integrationIdsInScope);
    }

    // None of these clauses were in the original filter,
    // the filter is already in a normalized state
    if (idRestrictions.isEmpty()) {
      return Optional.of(filter);
    }

    GetProjectIssueConfigurationsFilter.Builder builder =
        filter.toBuilder().clearIntegrationIds().clearIntegrationId().clearScope();

    return idRestrictions.stream()
        .reduce(Sets::intersection)
        .filter(Predicate.not(java.util.Collection::isEmpty))
        .map(builder::addAllIntegrationIds)
        .map(GetProjectIssueConfigurationsFilter.Builder::build);
  }
}
