package ai.traceable.api.gateway.config.service.delegate;

import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.api.gateway.config.service.store.ApiRoutesConfigStore;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.CreateRoutesResponse;
import ai.traceable.api.gateway.config.service.v1.NewApiRoute;
import ai.traceable.config.utils.UuidGenerator;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class ApiRoutesCreatorImpl implements ApiRoutesCreator {
  private final ApiRoutesConfigStore apiRoutesConfigStore;
  private final UuidGenerator uuidGenerator;

  @Override
  public CreateRoutesResponse create(
      final CreateRoutesRequest request, final RequestContext requestContext) {
    final List<ApiRoute> routesToCreate =
        request.getRoutesList().stream()
            .distinct()
            .map(this::buildApiRoute)
            .collect(toUnmodifiableList());
    final List<ContextualConfigObject<ApiRoute>> createdObjects =
        apiRoutesConfigStore.upsertObjects(requestContext, routesToCreate);
    final CreateRoutesResponse.Builder responseBuilder = CreateRoutesResponse.newBuilder();
    createdObjects.stream()
        .map(ContextualConfigObject::getData)
        .forEach(responseBuilder::addRoutes);
    return responseBuilder.build();
  }

  private ApiRoute buildApiRoute(final NewApiRoute newApiRoute) {
    return ApiRoute.newBuilder()
        .setId(uuidGenerator.generateId(newApiRoute))
        .setInfo(newApiRoute.getInfo())
        .setMetadata(newApiRoute.getMetadata())
        .build();
  }
}
