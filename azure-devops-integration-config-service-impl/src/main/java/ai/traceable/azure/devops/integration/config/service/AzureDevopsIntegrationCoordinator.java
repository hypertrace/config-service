package ai.traceable.azure.devops.integration.config.service;

import ai.traceable.azure.devops.integration.config.service.api.v1.*;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class AzureDevopsIntegrationCoordinator {
  private final AzureDevopsIntegrationStore azureDevopsIntegrationStore;

  public CreateAzureDevopsIntegrationResponse createAzureDevopsIntegration(
      CreateAzureDevopsIntegrationRequest request, RequestContext requestContext) {
    AzureDevopsIntegrationDetails.Builder integrationDetails =
        AzureDevopsIntegrationDetails.newBuilder()
            .setName(request.getName())
            .setOrganizationUrl(request.getOrganizationUrl());
    if (request.hasAzureDevopsIntegrationScope()) {
      integrationDetails.setAzureDevopsIntegrationScope(request.getAzureDevopsIntegrationScope());
    }
    if (request.hasDescription()) {
      integrationDetails.setDescription(request.getDescription());
    }
    String uuid = UUID.randomUUID().toString();
    AzureDevopsIntegrationWithAuthCredentials.Builder integrationWithAuthCredentialsBuilder =
        AzureDevopsIntegrationWithAuthCredentials.newBuilder()
            .setId(uuid)
            .setIntegrationDetails(integrationDetails)
            .setAuthCredentials(request.getAzureDevopsIntegrationAuthCredentials());

    azureDevopsIntegrationStore.upsertObject(
        requestContext, integrationWithAuthCredentialsBuilder.build());
    AzureDevopsIntegration.Builder integrationWithoutAuthCredentialsBuilder =
        AzureDevopsIntegration.newBuilder()
            .setId(uuid)
            .setAzureDevopsIntegrationDetails(integrationDetails);

    return CreateAzureDevopsIntegrationResponse.newBuilder()
        .setAzureDevopsIntegration(integrationWithoutAuthCredentialsBuilder)
        .build();
  }

  public List<AzureDevopsIntegration> getAzureDevopsIntegrations(
      GetAzureDevopsIntegrationsRequest request, RequestContext requestContext) {
    List<AzureDevopsIntegrationWithAuthCredentials> integrationsWithAuthCredentials =
        azureDevopsIntegrationStore.getAllConfigData(requestContext, request.getFilter());
    return integrationsWithAuthCredentials.stream()
        .map(
            integrationWithAuthCredentials ->
                AzureDevopsIntegration.newBuilder()
                    .setId(integrationWithAuthCredentials.getId())
                    .setAzureDevopsIntegrationDetails(
                        integrationWithAuthCredentials.getIntegrationDetails())
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  public AzureDevopsIntegrationWithAuthCredentials getAzureDevopsIntegrationWithAuthCredentials(
      GetAzureDevopsIntegrationWithAuthCredentialsRequest request, RequestContext requestContext) {
    return azureDevopsIntegrationStore
        .getData(requestContext, request.getIntegrationId())
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        "Unable to fetch AzureDevops-Integration with given Id as it does not exist")
                    .asRuntimeException());
  }

  public List<AzureDevopsIntegrationWithAuthCredentials>
      getAzureDevopsIntegrationsWithAuthCredentials(
          GetAzureDevopsIntegrationsWithAuthCredentialsRequest request,
          RequestContext requestContext) {
    return azureDevopsIntegrationStore.getAllConfigData(requestContext, request.getFilter());
  }

  public void deleteAzureDevopsIntegration(
      DeleteAzureDevopsIntegrationRequest request, RequestContext requestContext) {
    azureDevopsIntegrationStore
        .deleteObject(requestContext, request.getIntegrationId())
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        "Unable to delete AzureDevops-Integration with given Id as it does not exist")
                    .asRuntimeException());
  }

  public AzureDevopsIntegration updateAzureDevopsIntegration(
      UpdateAzureDevopsIntegrationRequest request, RequestContext requestContext) {
    AzureDevopsIntegrationWithAuthCredentials integrationWithAuthCredentials =
        azureDevopsIntegrationStore
            .getData(requestContext, request.getIntegrationId())
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription(
                            "Unable to update AzureDevops-Integration with given Id as it does not exist")
                        .asRuntimeException());

    AzureDevopsIntegrationWithAuthCredentials updatedIntegrationWithAuthCredentials =
        buildUpdatedIntegrationWithAuthCredentials(request, integrationWithAuthCredentials);
    azureDevopsIntegrationStore.upsertObject(requestContext, updatedIntegrationWithAuthCredentials);

    return AzureDevopsIntegration.newBuilder()
        .setId(request.getIntegrationId())
        .setAzureDevopsIntegrationDetails(
            updatedIntegrationWithAuthCredentials.getIntegrationDetails())
        .build();
  }

  private AzureDevopsIntegrationWithAuthCredentials buildUpdatedIntegrationWithAuthCredentials(
      UpdateAzureDevopsIntegrationRequest request,
      AzureDevopsIntegrationWithAuthCredentials integrationWithAuthCredentials) {
    AzureDevopsIntegrationWithAuthCredentials.Builder integrationWithAuthCredentialsBuilder =
        integrationWithAuthCredentials.toBuilder();
    AzureDevopsIntegrationDetails.Builder integrationDetailsAtBuilder =
        integrationWithAuthCredentialsBuilder.getIntegrationDetails().toBuilder();
    integrationDetailsAtBuilder.setName(request.getName());
    integrationDetailsAtBuilder.setDescription(request.getDescription());
    integrationDetailsAtBuilder.setOrganizationUrl(request.getOrganizationUrl());
    integrationDetailsAtBuilder.clearAzureDevopsIntegrationScope();
    if (request.hasAzureDevopsIntegrationScope()) {
      integrationDetailsAtBuilder.setAzureDevopsIntegrationScope(
          request.getAzureDevopsIntegrationScope());
    }
    integrationWithAuthCredentialsBuilder.setIntegrationDetails(integrationDetailsAtBuilder);
    if (request.hasAzureDevopsIntegrationAuthCredentials()) {
      integrationWithAuthCredentialsBuilder.setAuthCredentials(
          request.getAzureDevopsIntegrationAuthCredentials());
    }
    return integrationWithAuthCredentialsBuilder.build();
  }
}
