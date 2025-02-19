package ai.traceable.servicenow.itsm.integration.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.servicenow.itsm.integration.config.service.api.v1.CreateServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.DeleteServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationsWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmAuthCredentials;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationFilter;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationScope;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationWithAuthCredentials;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.UpdateServiceNowItsmIntegrationRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ServiceNowItsmIntegrationConfigServiceValidator {
  private final ServiceNowItsmIntegrationStore serviceNowItsmIntegrationStore;

  public void validateCreateServiceNowItsmIntegration(
      CreateServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, CreateServiceNowItsmIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, CreateServiceNowItsmIntegrationRequest.SERVER_URL_FIELD_NUMBER);
    validateUniqueNameOrThrow(requestContext, request.getName());
    if (request.hasScope()) {
      validateScopeForMutationOrThrow(request.getScope(), requestContext, Collections.emptySet());
    } else {
      validateUnscopedForMutationOrThrow(requestContext, Collections.emptySet());
    }
    validateServiceNowItsmAuthCredentials(request.getAuthCredentials());
  }

  public void validateGetServiceNowItsmIntegration(
      GetServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateServiceNowItsmIntegrationFilterOrThrow(request.getFilter());
  }

  public void validateGetServiceNowItsmIntegrationWithAuthCredentials(
      GetServiceNowItsmIntegrationWithAuthCredentialsRequest request,
      RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request,
        GetServiceNowItsmIntegrationWithAuthCredentialsRequest.INTEGRATION_ID_FIELD_NUMBER);
  }

  public void validateGetServiceNowItsmIntegrationsWithAuthCredentials(
      GetServiceNowItsmIntegrationsWithAuthCredentialsRequest request,
      RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateServiceNowItsmIntegrationFilterOrThrow(request.getFilter());
  }

  public void validateDeleteServiceNowItsmIntegration(
      DeleteServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteServiceNowItsmIntegrationRequest.INTEGRATION_ID_FIELD_NUMBER);
  }

  public void validateUpdateServiceNowItsmIntegration(
      UpdateServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateServiceNowItsmIntegrationRequest.INTEGRATION_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateServiceNowItsmIntegrationRequest.NAME_FIELD_NUMBER);
    if (request.hasScope()) {
      validateScopeForMutationOrThrow(
          request.getScope(), requestContext, Set.of(request.getIntegrationId()));
    } else {
      validateUnscopedForMutationOrThrow(requestContext, Set.of(request.getIntegrationId()));
    }
    validateUniqueNameOrThrow(requestContext, request.getName(), request.getIntegrationId());
    if (request.hasAuthCredentials()) {
      validateServiceNowItsmAuthCredentials(request.getAuthCredentials());
    }
  }

  private void validateServiceNowItsmAuthCredentials(
      ServiceNowItsmAuthCredentials authCredentials) {
    validateNonDefaultPresenceOrThrow(
        authCredentials, ServiceNowItsmAuthCredentials.USER_NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        authCredentials, ServiceNowItsmAuthCredentials.CLIENT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        authCredentials, ServiceNowItsmAuthCredentials.ENCRYPTION_KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        authCredentials, ServiceNowItsmAuthCredentials.ENCRYPTED_CLIENT_SECRET_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        authCredentials, ServiceNowItsmAuthCredentials.ENCRYPTED_USER_PASSWORD_FIELD_NUMBER);
  }

  private void validateUniqueNameOrThrow(RequestContext requestContext, String name) {
    if (serviceNowItsmIntegrationStore.getAllConfigData(requestContext).stream()
        .anyMatch(integration -> integration.getIntegrationDetails().getName().equals(name))) {
      throw getDuplicateNameStatusRuntimeException();
    }
  }

  private void validateUniqueNameOrThrow(
      RequestContext requestContext, String name, String integrationId) {
    if (serviceNowItsmIntegrationStore.getAllConfigData(requestContext).stream()
        .filter(integration -> !integrationId.equals(integration.getId()))
        .anyMatch(integration -> integration.getIntegrationDetails().getName().equals(name))) {
      throw getDuplicateNameStatusRuntimeException();
    }
  }

  private StatusRuntimeException getDuplicateNameStatusRuntimeException() {
    return ContextualStatusExceptionBuilder.from(
            Status.ALREADY_EXISTS.withDescription(
                "Already an existing servicenow itsm integration with the same name"))
        .useStatusDescriptionAsExternalMessage()
        .buildRuntimeException();
  }

  private void validateServiceNowItsmIntegrationFilterOrThrow(
      ServiceNowItsmIntegrationFilter filter) {
    if (filter.hasScope()) {
      validateScopeOrThrow(filter.getScope());
    }
  }

  private void validateScopeForMutationOrThrow(
      ServiceNowItsmIntegrationScope scope,
      RequestContext requestContext,
      Set<String> allowedServiceNowItsmIntegrationIds) {
    validateScopeOrThrow(scope);
    ServiceNowItsmIntegrationFilter filter =
        ServiceNowItsmIntegrationFilter.newBuilder().setScope(scope).build();
    if (containsOtherServiceNowItsmIntegration(
        serviceNowItsmIntegrationStore.getAllConfigData(requestContext, filter),
        allowedServiceNowItsmIntegrationIds)) {
      throw ContextualStatusExceptionBuilder.from(
              Status.ALREADY_EXISTS.withDescription(
                  "Given scope is not suitable for mutation as it conflicts with existing integrations."))
          .withExternalMessage("Environment scope conflicts with existing integrations.")
          .buildRuntimeException();
    }
  }

  private void validateUnscopedForMutationOrThrow(
      RequestContext requestContext, Set<String> allowedServiceNowItsmIntegrationIds) {
    if (containsOtherServiceNowItsmIntegration(
        serviceNowItsmIntegrationStore.getAllConfigData(requestContext),
        allowedServiceNowItsmIntegrationIds)) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT.withDescription(
                  "Unscoped integration cannot be persisted due to conflicts with existing integrations."))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  private boolean containsOtherServiceNowItsmIntegration(
      List<ServiceNowItsmIntegrationWithAuthCredentials> serviceNowItsmIntegrations,
      Set<String> allowedServiceNowItsmIntegrationIds) {
    return !allowedServiceNowItsmIntegrationIds.containsAll(
        serviceNowItsmIntegrations.stream()
            .map(ServiceNowItsmIntegrationWithAuthCredentials::getId)
            .collect(Collectors.toUnmodifiableList()));
  }

  private void validateScopeOrThrow(ServiceNowItsmIntegrationScope scope) {
    switch (scope.getScopeCase()) {
      case ENVIRONMENT_IDS:
        if (scope.getEnvironmentIds().getValuesList().isEmpty()) {
          throw ContextualStatusExceptionBuilder.from(
                  Status.INVALID_ARGUMENT.withDescription(
                      "Environment-ids in Scope cannot be an empty list."))
              .withExternalMessage("Please provide at least one environment ID in the scope")
              .buildRuntimeException();
        } else {
          scope
              .getEnvironmentIds()
              .getValuesList()
              .forEach(
                  environmentId -> {
                    if (environmentId.isEmpty()) {
                      throw ContextualStatusExceptionBuilder.from(
                              Status.INVALID_ARGUMENT.withDescription(
                                  "Environment-id cannot be empty string"))
                          .useStatusDescriptionAsExternalMessage()
                          .buildRuntimeException();
                    }
                  });
        }
        break;
      case SCOPE_NOT_SET:
      default:
        throw ContextualStatusExceptionBuilder.from(
                Status.INVALID_ARGUMENT.withDescription("Invalid Scope"))
            .useStatusDescriptionAsExternalMessage()
            .buildRuntimeException();
    }
  }
}
