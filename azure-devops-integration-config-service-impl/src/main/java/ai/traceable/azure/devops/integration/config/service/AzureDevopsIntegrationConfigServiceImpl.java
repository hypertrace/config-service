package ai.traceable.azure.devops.integration.config.service;

import ai.traceable.azure.devops.integration.config.service.api.v1.AzureDevopsIntegrationConfigServiceGrpc;
import ai.traceable.azure.devops.integration.config.service.api.v1.CreateAzureDevopsIntegrationRequest;
import ai.traceable.azure.devops.integration.config.service.api.v1.CreateAzureDevopsIntegrationResponse;
import ai.traceable.azure.devops.integration.config.service.api.v1.DeleteAzureDevopsIntegrationRequest;
import ai.traceable.azure.devops.integration.config.service.api.v1.DeleteAzureDevopsIntegrationResponse;
import ai.traceable.azure.devops.integration.config.service.api.v1.GetAzureDevopsIntegrationWithAuthCredentialsRequest;
import ai.traceable.azure.devops.integration.config.service.api.v1.GetAzureDevopsIntegrationWithAuthCredentialsResponse;
import ai.traceable.azure.devops.integration.config.service.api.v1.GetAzureDevopsIntegrationsRequest;
import ai.traceable.azure.devops.integration.config.service.api.v1.GetAzureDevopsIntegrationsResponse;
import ai.traceable.azure.devops.integration.config.service.api.v1.GetAzureDevopsIntegrationsWithAuthCredentialsRequest;
import ai.traceable.azure.devops.integration.config.service.api.v1.GetAzureDevopsIntegrationsWithAuthCredentialsResponse;
import ai.traceable.azure.devops.integration.config.service.api.v1.UpdateAzureDevopsIntegrationRequest;
import ai.traceable.azure.devops.integration.config.service.api.v1.UpdateAzureDevopsIntegrationResponse;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AzureDevopsIntegrationConfigServiceImpl
    extends AzureDevopsIntegrationConfigServiceGrpc.AzureDevopsIntegrationConfigServiceImplBase {
  private final AzureDevopsIntegrationCoordinator coordinator;
  private final AzureDevopsIntegrationConfigServiceValidator validator;

  @Inject
  public AzureDevopsIntegrationConfigServiceImpl(
      AzureDevopsIntegrationConfigServiceValidator validator,
      AzureDevopsIntegrationCoordinator coordinator) {
    this.validator = validator;
    this.coordinator = coordinator;
  }

  @Override
  public void createAzureDevopsIntegration(
      CreateAzureDevopsIntegrationRequest request,
      StreamObserver<CreateAzureDevopsIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateCreateAzureDevopsIntegration(request, requestContext);
      responseObserver.onNext(coordinator.createAzureDevopsIntegration(request, requestContext));
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during creating integration for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  //   GET Api for fetching all integrations without auth credentials for a given filter\
  @Override
  public void getAzureDevopsIntegrations(
      GetAzureDevopsIntegrationsRequest request,
      StreamObserver<GetAzureDevopsIntegrationsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateGetAzureDevopsIntegrations(request, requestContext);
      responseObserver.onNext(
          GetAzureDevopsIntegrationsResponse.newBuilder()
              .addAllAzureDevopsIntegration(
                  coordinator.getAzureDevopsIntegrations(request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during fetching integrations for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  // GET Api for fetching integration with auth credential for a given integration id
  @Override
  public void getAzureDevopsIntegrationWithAuthCredentials(
      GetAzureDevopsIntegrationWithAuthCredentialsRequest request,
      StreamObserver<GetAzureDevopsIntegrationWithAuthCredentialsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateGetAzureDevopsIntegrationWithAuthCredentials(request, requestContext);
      responseObserver.onNext(
          GetAzureDevopsIntegrationWithAuthCredentialsResponse.newBuilder()
              .setAzureDevopsIntegrationWithAuthCredentials(
                  coordinator.getAzureDevopsIntegrationWithAuthCredentials(request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during fetching integrations for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  // GET Api for fetching all integrations with auth credentials for a given filter
  @Override
  public void getAzureDevopsIntegrationsWithAuthCredentials(
      GetAzureDevopsIntegrationsWithAuthCredentialsRequest request,
      StreamObserver<GetAzureDevopsIntegrationsWithAuthCredentialsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateGetAzureDevopsIntegrationsWithAuthCredentials(request, requestContext);
      responseObserver.onNext(
          GetAzureDevopsIntegrationsWithAuthCredentialsResponse.newBuilder()
              .addAllAzureDevopsIntegrationWithAuthCredentials(
                  coordinator.getAzureDevopsIntegrationsWithAuthCredentials(
                      request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during fetching integrations for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateAzureDevopsIntegration(
      UpdateAzureDevopsIntegrationRequest request,
      StreamObserver<UpdateAzureDevopsIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateUpdateAzureDevopsIntegration(request, requestContext);
      responseObserver.onNext(
          UpdateAzureDevopsIntegrationResponse.newBuilder()
              .setAzureDevopsIntegration(
                  coordinator.updateAzureDevopsIntegration(request, requestContext))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during updating integration for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteAzureDevopsIntegration(
      DeleteAzureDevopsIntegrationRequest request,
      StreamObserver<DeleteAzureDevopsIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateDeleteAzureDevopsIntegration(request, requestContext);
      coordinator.deleteAzureDevopsIntegration(request, requestContext);
      responseObserver.onNext(DeleteAzureDevopsIntegrationResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed during deleting integration for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }
}
