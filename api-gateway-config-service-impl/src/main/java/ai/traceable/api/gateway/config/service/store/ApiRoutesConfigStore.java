package ai.traceable.api.gateway.config.service.store;

import ai.traceable.api.gateway.config.service.filter.ApiRouteFilterToPredicateConverter;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter.TypeCase;
import com.google.protobuf.Value;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class ApiRoutesConfigStore
    extends IdentifiedObjectStoreWithFilter<ApiRoute, ApiRouteFilter> {
  private static final String API_GATEWAY_CONFIG_RESOURCE_NAME = "api-gateway-route";
  private static final String API_GATEWAY_CONFIG_RESOURCE_NAMESPACE = "api-gateway";
  private final Map<TypeCase, ApiRouteFilterToPredicateConverter> filterConverterMap;

  @Inject
  public ApiRoutesConfigStore(
      final ConfigServiceBlockingStub configServiceBlockingStub,
      final ConfigChangeEventGenerator configChangeEventGenerator,
      final Map<TypeCase, ApiRouteFilterToPredicateConverter> filterConverterMap) {
    super(
        configServiceBlockingStub,
        API_GATEWAY_CONFIG_RESOURCE_NAMESPACE,
        API_GATEWAY_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.filterConverterMap = filterConverterMap;
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
    final ApiRouteFilter.TypeCase filterCase = filter.getTypeCase();
    final ApiRouteFilterToPredicateConverter converter = filterConverterMap.get(filterCase);

    if (converter == null) {
      log.error("Unhandled filter case: " + filterCase);
      return Optional.empty();
    }

    return Optional.of(apiRoute).filter(converter.convert(filter));
  }
}
