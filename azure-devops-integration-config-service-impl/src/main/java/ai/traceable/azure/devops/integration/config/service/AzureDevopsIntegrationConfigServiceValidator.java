package ai.traceable.azure.devops.integration.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.azure.devops.integration.config.service.api.v1.*;
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
public class AzureDevopsIntegrationConfigServiceValidator {
  private final AzureDevopsIntegrationStore azureDevopsIntegrationStore;

  public void validateCreateAzureDevopsIntegration(
      CreateAzureDevopsIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, CreateAzureDevopsIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, CreateAzureDevopsIntegrationRequest.ORGANIZATION_URL_FIELD_NUMBER);
    validateUniqueNameOrThrow(requestContext, request.getName());
    if (request.hasAzureDevopsIntegrationScope()) {
      validateScopeForMutationOrThrow(
          request.getAzureDevopsIntegrationScope(), requestContext, Collections.emptySet());
    } else {
      validateUnscopedForMutationOrThrow(requestContext, Collections.emptySet());
    }

    validateAzureDevopsIntegrationAuthCredentials(
        request.getAzureDevopsIntegrationAuthCredentials());
  }

  public void validateGetAzureDevopsIntegrations(
      GetAzureDevopsIntegrationsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateAzureDevopsIntegrationFilterOrThrow(request.getFilter());
  }

  public void validateGetAzureDevopsIntegrationWithAuthCredentials(
      GetAzureDevopsIntegrationWithAuthCredentialsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, GetAzureDevopsIntegrationWithAuthCredentialsRequest.INTEGRATION_ID_FIELD_NUMBER);
  }

  public void validateGetAzureDevopsIntegrationsWithAuthCredentials(
      GetAzureDevopsIntegrationsWithAuthCredentialsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateAzureDevopsIntegrationFilterOrThrow(request.getFilter());
  }

  public void validateDeleteAzureDevopsIntegration(
      DeleteAzureDevopsIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteAzureDevopsIntegrationRequest.INTEGRATION_ID_FIELD_NUMBER);
  }

  public void validateUpdateAzureDevopsIntegration(
      UpdateAzureDevopsIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateAzureDevopsIntegrationRequest.INTEGRATION_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateAzureDevopsIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateAzureDevopsIntegrationRequest.ORGANIZATION_URL_FIELD_NUMBER);
    if (request.hasAzureDevopsIntegrationScope()) {
      validateScopeForMutationOrThrow(
          request.getAzureDevopsIntegrationScope(),
          requestContext,
          Set.of(request.getIntegrationId()));
    } else {
      validateUnscopedForMutationOrThrow(requestContext, Set.of(request.getIntegrationId()));
    }
    // validate name doesn't exists in other tenant integrations
    validateUniqueNameOrThrow(requestContext, request.getName(), request.getIntegrationId());
    if (request.hasAzureDevopsIntegrationAuthCredentials()) {
      validateAzureDevopsIntegrationAuthCredentials(
          request.getAzureDevopsIntegrationAuthCredentials());
    }
  }

  private void validateAzureDevopsIntegrationAuthCredentials(
      AzureDevopsIntegrationAuthCredentials integrationAuthCredentials) {
    validateNonDefaultPresenceOrThrow(
        integrationAuthCredentials,
        AzureDevopsIntegrationAuthCredentials.ENCRYPTED_PAT_FIELD_NUMBER);
  }

  private void validateUniqueNameOrThrow(RequestContext requestContext, String name) {
    if (azureDevopsIntegrationStore.getAllConfigData(requestContext).stream()
        .anyMatch(integration -> integration.getIntegrationDetails().getName().equals(name))) {
      throw getDuplicateNameStatusRuntimeException();
    }
  }

  private void validateUniqueNameOrThrow(
      RequestContext requestContext, String name, String integrationId) {
    if (azureDevopsIntegrationStore.getAllConfigData(requestContext).stream()
        .filter(integration -> !integrationId.equals(integration.getId()))
        .anyMatch(integration -> integration.getIntegrationDetails().getName().equals(name))) {
      throw getDuplicateNameStatusRuntimeException();
    }
  }

  private void validateIntegrationIdExists(RequestContext requestContext, String integrationId) {
    if (azureDevopsIntegrationStore.getAllConfigData(requestContext).stream()
        .noneMatch(integration -> integration.getId().equals(integrationId))) {
      throw getInvalidIntegrationIdRuntimeException();
    }
  }

  private StatusRuntimeException getDuplicateNameStatusRuntimeException() {
    return Status.INVALID_ARGUMENT
        .withDescription("Already an existing azure devops integration with the same name")
        .asRuntimeException();
  }

  private StatusRuntimeException getInvalidIntegrationIdRuntimeException() {
    return Status.INVALID_ARGUMENT
        .withDescription("No Azure Integration with given integration id exists")
        .asRuntimeException();
  }

  private void validateScopeForMutationOrThrow(
      AzureDevopsIntegrationScope scope,
      RequestContext requestContext,
      Set<String> allowedAzureDevopsIntegrationIds) {
    validateScopeOrThrow(scope);
    AzureDevopsIntegrationFilter filter =
        AzureDevopsIntegrationFilter.newBuilder().setScope(scope).build();
    if (containsOtherAzureDevopsIntegration(
        azureDevopsIntegrationStore.getAllConfigData(requestContext, filter),
        allowedAzureDevopsIntegrationIds)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Given scope is not suitable for mutation as it conflicts with existing integrations.")
          .asRuntimeException();
    }
  }

  private void validateAzureDevopsIntegrationFilterOrThrow(AzureDevopsIntegrationFilter filter) {
    if (filter.hasScope()) {
      validateScopeOrThrow(filter.getScope());
    }
  }

  private void validateUnscopedForMutationOrThrow(
      RequestContext requestContext, Set<String> allowedAzureDevopsIntegrationIds) {
    if (containsOtherAzureDevopsIntegration(
        azureDevopsIntegrationStore.getAllConfigData(requestContext),
        allowedAzureDevopsIntegrationIds)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Cannot persist an unscoped integration as it will conflict with one or more existing integrations.")
          .asRuntimeException();
    }
  }

  private boolean containsOtherAzureDevopsIntegration(
      List<AzureDevopsIntegrationWithAuthCredentials> azureDevopsIntegrations,
      Set<String> allowedAzureDevopsIntegrationIds) {
    return !allowedAzureDevopsIntegrationIds.containsAll(
        azureDevopsIntegrations.stream()
            .map(AzureDevopsIntegrationWithAuthCredentials::getId)
            .collect(Collectors.toUnmodifiableList()));
  }

  private void validateScopeOrThrow(AzureDevopsIntegrationScope scope) {
    switch (scope.getScopeCase()) {
      case ENVIRONMENT_IDS:
        if (scope.getEnvironmentIds().getValuesList().isEmpty()) {
          throw Status.INVALID_ARGUMENT
              .withDescription("Environment-ids in Scope cannot be an empty list")
              .asRuntimeException();
        }
        break;
      case SCOPE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT.withDescription("Invalid Scope").asRuntimeException();
    }
  }
}
