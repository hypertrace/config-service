package ai.traceable.api.gateway.config.service.delegate;

import ai.traceable.api.gateway.config.service.store.ApiRoutesConfigStore;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesResponse;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ApiRoutesGetterImpl implements ApiRoutesGetter {
  private final ApiRoutesConfigStore apiRoutesConfigStore;

  @Override
  public GetRoutesResponse get(
      final GetRoutesRequest request, final RequestContext requestContext) {
    final List<ApiRoute> apiRoutesToReturn =
        apiRoutesConfigStore.getAllConfigData(requestContext, request.getFilter());
    return GetRoutesResponse.newBuilder().addAllRoutes(apiRoutesToReturn).build();
  }
}
