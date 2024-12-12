package ai.traceable.github.integration.config.service;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.github.integration.config.service.v1.CreateGithubIntegrationRequest;
import ai.traceable.github.integration.config.service.v1.CreateGithubIntegrationResponse;
import ai.traceable.github.integration.config.service.v1.DeleteGithubIntegrationRequest;
import ai.traceable.github.integration.config.service.v1.DeleteGithubIntegrationResponse;
import ai.traceable.github.integration.config.service.v1.GetGithubIntegrationsRequest;
import ai.traceable.github.integration.config.service.v1.GetGithubIntegrationsResponse;
import ai.traceable.github.integration.config.service.v1.GithubIntegration;
import ai.traceable.github.integration.config.service.v1.GithubIntegrationConfigServiceGrpc;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus.AwaitingRequest;
import ai.traceable.github.integration.config.service.v1.UpdateGithubIntegrationRequest;
import ai.traceable.github.integration.config.service.v1.UpdateGithubIntegrationResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
class GithubIntegrationConfigServiceImpl
    extends GithubIntegrationConfigServiceGrpc.GithubIntegrationConfigServiceImplBase {
  private final GithubIntegrationConfigServiceValidator validator;
  private final GithubIntegrationStore store;
  private final UuidGenerator uuidGenerator;

  @Override
  public void getGithubIntegrations(
      GetGithubIntegrationsRequest request,
      StreamObserver<GetGithubIntegrationsResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateGetGithubIntegrations(request, requestContext);
      final List<GithubIntegration> integrations;
      if (request.hasFilter()) {
        integrations = store.getAllConfigData(requestContext, request.getFilter());
      } else {
        integrations = store.getAllConfigData(requestContext);
      }
      responseObserver.onNext(
          GetGithubIntegrationsResponse.newBuilder().addAllIntegrations(integrations).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed to get github integrations for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void createGithubIntegration(
      CreateGithubIntegrationRequest request,
      StreamObserver<CreateGithubIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateCreateGithubIntegration(request, requestContext);
      GithubIntegration newIntegration =
          GithubIntegration.newBuilder()
              .setId(uuidGenerator.generateRandomId())
              .setStatus(
                  IntegrationStatus.newBuilder()
                      .setAwaitingRequest(
                          AwaitingRequest.newBuilder()
                              .setRequestUserEmail(requestContext.getEmail().orElseThrow())))
              .build();
      responseObserver.onNext(
          CreateGithubIntegrationResponse.newBuilder()
              .setCreatedIntegration(store.upsertObject(requestContext, newIntegration).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed to create github integration for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateGithubIntegration(
      UpdateGithubIntegrationRequest request,
      StreamObserver<UpdateGithubIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateUpdateGithubIntegration(request, requestContext);
      GithubIntegration existingIntegration =
          store
              .getData(requestContext, request.getId())
              .orElseThrow(
                  () ->
                      Status.NOT_FOUND
                          .withDescription("Github integration not found")
                          .asRuntimeException());
      GithubIntegration integrationUpdate =
          existingIntegration.toBuilder().setStatus(request.getStatus()).build();
      validator.validateUpdateValidForStatusChange(
          existingIntegration.getStatus(), integrationUpdate.getStatus());
      responseObserver.onNext(
          UpdateGithubIntegrationResponse.newBuilder()
              .setUpdatedIntegration(
                  store.upsertObject(requestContext, integrationUpdate).getData())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed to update github integration for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void deleteGithubIntegration(
      DeleteGithubIntegrationRequest request,
      StreamObserver<DeleteGithubIntegrationResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    try {
      validator.validateDeleteGithubIntegration(request, requestContext);
      store.deleteObject(requestContext, request.getId());
      responseObserver.onNext(DeleteGithubIntegrationResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error(
          "Failed to delete github integration for request: {} within context: {}",
          request,
          requestContext,
          e);
      responseObserver.onError(e);
    }
  }
}
