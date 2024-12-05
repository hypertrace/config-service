package ai.traceable.github.integration.config.service;

import ai.traceable.github.integration.config.service.v1.GetGithubIntegrationsRequest;
import ai.traceable.github.integration.config.service.v1.GetGithubIntegrationsResponse;
import ai.traceable.github.integration.config.service.v1.GithubIntegration;
import ai.traceable.github.integration.config.service.v1.GithubIntegrationConfigServiceGrpc;
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
}
