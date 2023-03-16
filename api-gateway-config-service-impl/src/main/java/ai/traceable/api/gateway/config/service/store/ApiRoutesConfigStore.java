package ai.traceable.api.gateway.config.service.store;

import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class ApiRoutesConfigStore
    extends IdentifiedObjectStoreWithFilter<ApiRoute, ApiRouteFilter> {
  private static final String API_GATEWAY_CONFIG_RESOURCE_NAME = "api-gateway-route";
  private static final String API_GATEWAY_CONFIG_RESOURCE_NAMESPACE = "api-gateway";

  @Inject
  public ApiRoutesConfigStore(
      final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      final ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        API_GATEWAY_CONFIG_RESOURCE_NAMESPACE,
        API_GATEWAY_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<ApiRoute> buildDataFromValue(final Value value) {
    final ApiRoute.Builder configBuilder = ApiRoute.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, configBuilder);
    return Optional.of(configBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(final ApiRoute apiRoute) {
    return ConfigProtoConverter.convertToValue(apiRoute);
  }

  @Override
  protected String getContextFromData(final ApiRoute apiRoute) {
    return apiRoute.getId();
  }

  @Override
  protected Optional<ApiRoute> filterConfigData(
      final ApiRoute apiRoute, final ApiRouteFilter filter) {
    final Optional<ApiRoute> optionalRoute = Optional.of(apiRoute);
    if (ApiRouteFilter.getDefaultInstance().equals(filter)) {
      return optionalRoute;
    }

    if (filter.hasOrgIds()) {
      return optionalRoute.filter(
          route -> filter.getOrgIds().getOrgIdList().contains(route.getMetadata().getOrgId()));
    }

    log.error("Unhandled filter case: " + filter.getTypeCase());
    return Optional.empty();
  }
}
