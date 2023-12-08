package ai.traceable.jira.integration.config.service;

import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationResponse;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegration;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.UUID;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class JiraIntegrationCoordinator {
  private final JiraIntegrationStore jiraIntegrationStore;

  @Inject
  JiraIntegrationCoordinator(JiraIntegrationStore jiraIntegrationStore) {
    this.jiraIntegrationStore = jiraIntegrationStore;
  }

  public CreateJiraIntegrationResponse createJiraIntegration(
      CreateJiraIntegrationRequest request, RequestContext requestContext) {
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
    jiraIntegrationStore.upsertObject(requestContext, jiraIntegration.build());
    return CreateJiraIntegrationResponse.newBuilder().setIntegration(jiraIntegration).build();
  }

  public List<JiraIntegration> getJiraIntegration(
      RequestContext requestContext, GetJiraIntegrationsRequest request) {
    if (request.hasJiraIntegrationFilter()) {
      return jiraIntegrationStore.getAllConfigData(
          requestContext, request.getJiraIntegrationFilter());
    }
    return jiraIntegrationStore.getAllConfigData(requestContext);
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
}
