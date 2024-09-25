package ai.traceable.servicenow.itsm.integration.config.service;

import ai.traceable.servicenow.itsm.integration.config.service.api.v1.CreateServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.CreateServiceNowItsmIntegrationResponse;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.DeleteServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationsWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowIntegrationDetails;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegration;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationWithAuthCredentials;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.UpdateServiceNowItsmIntegrationRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ServiceNowItsmIntegrationCoordinator {
  private final ServiceNowItsmIntegrationStore serviceNowItsmIntegrationStore;

  public CreateServiceNowItsmIntegrationResponse createServiceNowItsmIntegration(
      CreateServiceNowItsmIntegrationRequest request, RequestContext requestContext) {

    ServiceNowIntegrationDetails.Builder integrationDetails =
        ServiceNowIntegrationDetails.newBuilder()
            .setName(request.getName())
            .setServerUrl(request.getServerUrl());
    if (request.hasScope()) {
      integrationDetails.setScope(request.getScope());
    }
    if (request.hasDescription()) {
      integrationDetails.setDescription(request.getDescription());
    }
    String uuid = UUID.randomUUID().toString();
    ServiceNowItsmIntegrationWithAuthCredentials.Builder integrationWithAuthCredentialsBuilder =
        ServiceNowItsmIntegrationWithAuthCredentials.newBuilder()
            .setId(uuid)
            .setIntegrationDetails(integrationDetails)
            .setAuthCredentials(request.getAuthCredentials());

    serviceNowItsmIntegrationStore.upsertObject(
        requestContext, integrationWithAuthCredentialsBuilder.build());
    ServiceNowItsmIntegration.Builder integrationWithoutAuthCredentialsBuilder =
        ServiceNowItsmIntegration.newBuilder()
            .setId(uuid)
            .setIntegrationDetails(integrationDetails);

    return CreateServiceNowItsmIntegrationResponse.newBuilder()
        .setServiceNowItsmIntegration(integrationWithoutAuthCredentialsBuilder)
        .build();
  }

  public List<ServiceNowItsmIntegration> getServiceNowItsmIntegration(
      GetServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    List<ServiceNowItsmIntegrationWithAuthCredentials> integrationsWithAuthCredentials =
        serviceNowItsmIntegrationStore.getAllConfigData(requestContext, request.getFilter());
    return integrationsWithAuthCredentials.stream()
        .map(
            integrationWithAuthCredentials ->
                ServiceNowItsmIntegration.newBuilder()
                    .setId(integrationWithAuthCredentials.getId())
                    .setIntegrationDetails(integrationWithAuthCredentials.getIntegrationDetails())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  public ServiceNowItsmIntegrationWithAuthCredentials
      getServiceNowItsmIntegrationWithAuthCredentials(
          GetServiceNowItsmIntegrationWithAuthCredentialsRequest request,
          RequestContext requestContext) {
    return serviceNowItsmIntegrationStore
        .getData(requestContext, request.getIntegrationId())
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        "Unable to fetch ServiceNow-Integration with given Id as it does not exist")
                    .asRuntimeException());
  }

  public List<ServiceNowItsmIntegrationWithAuthCredentials>
      getServiceNowItsmIntegrationsWithAuthCredentials(
          GetServiceNowItsmIntegrationsWithAuthCredentialsRequest request,
          RequestContext requestContext) {
    return serviceNowItsmIntegrationStore.getAllConfigData(requestContext, request.getFilter());
  }

  public void deleteServiceNowItsmIntegration(
      DeleteServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    serviceNowItsmIntegrationStore
        .deleteObject(requestContext, request.getIntegrationId())
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        "Unable to delete ServiceNow-Integration with given Id as it does not exist")
                    .asRuntimeException());
  }

  public ServiceNowItsmIntegration updateServiceNowItsmIntegration(
      UpdateServiceNowItsmIntegrationRequest request, RequestContext requestContext) {
    ServiceNowItsmIntegrationWithAuthCredentials.Builder integrationWithAuthCredentialsBuilder =
        serviceNowItsmIntegrationStore
            .getData(requestContext, request.getIntegrationId())
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription(
                            "Unable to update ServiceNow-Integration with given Id as it does not exist")
                        .asRuntimeException())
            .toBuilder();
    ServiceNowIntegrationDetails.Builder integrationDetailsAtBuilder =
        integrationWithAuthCredentialsBuilder.getIntegrationDetails().toBuilder();
    integrationDetailsAtBuilder.setName(request.getName());
    integrationDetailsAtBuilder.setDescription(request.getDescription());
    integrationDetailsAtBuilder.setServerUrl(request.getServerUrl());
    integrationDetailsAtBuilder.clearScope();
    if (request.hasScope()) {
      integrationDetailsAtBuilder.setScope(request.getScope());
    }
    integrationWithAuthCredentialsBuilder.setIntegrationDetails(integrationDetailsAtBuilder);
    if (request.hasAuthCredentials()) {
      integrationWithAuthCredentialsBuilder.setAuthCredentials(request.getAuthCredentials());
    }
    serviceNowItsmIntegrationStore.upsertObject(
        requestContext, integrationWithAuthCredentialsBuilder.build());
    return ServiceNowItsmIntegration.newBuilder()
        .setId(request.getIntegrationId())
        .setIntegrationDetails(integrationDetailsAtBuilder)
        .build();
  }
}
