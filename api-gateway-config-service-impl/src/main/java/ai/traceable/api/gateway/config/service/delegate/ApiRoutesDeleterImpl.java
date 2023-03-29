package ai.traceable.api.gateway.config.service.delegate;

import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.api.gateway.config.service.store.ApiRoutesConfigStore;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.DeleteRoutesResponse;
import java.util.List;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ApiRoutesDeleterImpl implements ApiRoutesDeleter {
  private final ApiRoutesConfigStore apiRoutesConfigStore;

  @Override
  public DeleteRoutesResponse delete(
      final DeleteRoutesRequest request, final RequestContext requestContext) {
    final List<ApiRoute> apiRoutesToDelete =
        apiRoutesConfigStore.getAllConfigData(requestContext, request.getFilter());
    final List<String> ids =
        apiRoutesToDelete.stream().map(ApiRoute::getId).collect(toUnmodifiableList());
    if (!ids.isEmpty()) {
      apiRoutesConfigStore.deleteObjects(requestContext, ids);
    }
    return DeleteRoutesResponse.newBuilder().build();
  }
}
