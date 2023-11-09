package ai.traceable.jira.integration.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.EncryptedData;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationFilter;
import ai.traceable.jira.integration.config.service.api.v1.Scope;
import ai.traceable.jira.integration.config.service.api.v1.StringList;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class JiraIntegrationConfigServiceValidator {
  private final JiraIntegrationStore jiraIntegrationStore;

  public void validateCreateJiraIntegration(
      CreateJiraIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateJiraIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, CreateJiraIntegrationRequest.CONSUMER_KEY_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, CreateJiraIntegrationRequest.BASE_URL_FIELD_NUMBER);
    validateEncryptedDataOrThrow(request.getEncryptedAccessToken());
    if (request.hasScope()) {
      validateScopeOrThrow(request.getScope());
      if (request.getScope().hasEnvironmentIds()) {
        assertNoEnvironmentOverlapOrThrow(
            request.getScope().getEnvironmentIds().getValuesList(), requestContext);
      }
    } else {
      // scope not set means environment list is empty
      assertNoEnvironmentOverlapOrThrow(List.of(), requestContext);
    }
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
      validateScopeOrThrow(request.getScope());
    }
  }

  public void validateDeleteJiraIntegration(
      DeleteJiraIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteJiraIntegrationRequest.JIRA_INTEGRATION_ID_FIELD_NUMBER);
  }

  private void assertNoEnvironmentOverlapOrThrow(
      List<String> environments, RequestContext requestContext) {
    if (!jiraIntegrationStore
        .getAllConfigData(
            requestContext,
            JiraIntegrationFilter.newBuilder()
                .setFilterScope(
                    Scope.newBuilder()
                        .setEnvironmentIds(StringList.newBuilder().addAllValues(environments)))
                .build())
        .isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "The provided set of Environments overlap with one or more existing Jira-Integrations")
          .asRuntimeException();
    }
  }
}
