package ai.traceable.api.gateway.config.cache;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.api.gateway.config.service.filter.ApiGatewayFilterModule;
import ai.traceable.api.gateway.config.service.filter.ApiRouteFilterToPredicateConverter;
import ai.traceable.api.gateway.config.service.v1.ApiGatewayConfigServiceGrpc;
import ai.traceable.api.gateway.config.service.v1.ApiGatewayConfigServiceGrpc.ApiGatewayConfigServiceBlockingStub;
import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter.TypeCase;
import ai.traceable.api.gateway.config.service.v1.GetRoutesRequest;
import ai.traceable.api.gateway.config.service.v1.GetRoutesResponse;
import ai.traceable.platform.event.invalidation.cache.ChangeEventConsumer;
import ai.traceable.platform.event.invalidation.cache.TimedCacheWithChangeEventInvalidation;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import lombok.NonNull;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKeyDeserializer;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValueDeserializer;
import org.hypertrace.core.grpcutils.context.RequestContext;

@SuppressWarnings("unused")
public class DefaultCachedApiRouteProvider
    extends TimedCacheWithChangeEventInvalidation<
        String, List<ApiRoute>, ConfigChangeEventKey, ConfigChangeEventValue>
    implements CachedApiRouteProvider {
  private static final String CACHE_NAME = DefaultCachedApiRouteProvider.class.getSimpleName();

  private final ApiGatewayConfigServiceGrpc.ApiGatewayConfigServiceBlockingStub
      gatewayConfigServiceClient;
  private final Map<TypeCase, ApiRouteFilterToPredicateConverter> predicateConverterMap;
  private final Duration timeout;

  DefaultCachedApiRouteProvider(final GatewayConfigServiceCacheConfig config) {
    super(
        CACHE_NAME,
        config.getKafkaConfig(),
        Clock.systemUTC(),
        config.getTimedCacheConfig(),
        new ConfigChangeEventKeyDeserializer(),
        new ConfigChangeEventValueDeserializer());
    this.gatewayConfigServiceClient = buildGatewayConfigServiceClient(config);
    this.predicateConverterMap = getPredicateConverterMap();
    this.timeout = config.getConnectionDetails().getTimeout();
  }

  // TODO:
  //  1. Spin-up docker container and make the tests like integration tests, then this constructor
  // can go away
  //  2. Alternatively, make the library code of 'TimedCacheWithChangeEventInvalidation' more
  // testable
  @VisibleForTesting
  DefaultCachedApiRouteProvider(
      final GatewayConfigServiceCacheConfig config,
      final ChangeEventConsumer<String, List<ApiRoute>> changeEventConsumer,
      final ApiGatewayConfigServiceGrpc.ApiGatewayConfigServiceBlockingStub
          gatewayConfigServiceClient) {
    super(CACHE_NAME, Clock.systemUTC(), config.getTimedCacheConfig(), changeEventConsumer);
    this.gatewayConfigServiceClient = gatewayConfigServiceClient;
    this.predicateConverterMap = getPredicateConverterMap();
    this.timeout = config.getConnectionDetails().getTimeout();
  }

  @Override
  public List<ApiRoute> getRoutes(final ApiRouteFilter filter, final RequestContext context) {
    final List<ApiRoute> allRoutes = getValue(context.getTenantId().orElseThrow());
    final Predicate<ApiRoute> predicate =
        predicateConverterMap.get(filter.getTypeCase()).convert(filter);
    return allRoutes.stream().filter(predicate).collect(toUnmodifiableList());
  }

  @Override
  protected List<String> getKeys(
      final ConfigChangeEventKey configChangeEventKey,
      final ConfigChangeEventValue configChangeEventValue) {
    if (!ApiRoute.class.getName().equals(configChangeEventKey.getConfigType())) {
      return emptyList();
    }

    return List.of(configChangeEventKey.getTenantId());
  }

  @Override
  protected List<ApiRoute> loadValue(@NonNull final String tenantId) {
    final GetRoutesRequest request = GetRoutesRequest.getDefaultInstance();
    final RequestContext context = RequestContext.forTenantId(tenantId);
    final GetRoutesResponse response =
        context.call(
            () ->
                gatewayConfigServiceClient
                    .withDeadlineAfter(timeout.toMillis(), MILLISECONDS)
                    .getRoutes(request));
    return response.getRoutesList();
  }

  @Override
  protected boolean isValueNotLoaded(final List<ApiRoute> apiRoutes) {
    return Objects.isNull(apiRoutes);
  }

  @SuppressWarnings("Convert2Diamond")
  private Map<TypeCase, ApiRouteFilterToPredicateConverter> getPredicateConverterMap() {
    return Guice.createInjector(new ApiGatewayFilterModule())
        .getInstance(
            Key.get(new TypeLiteral<Map<TypeCase, ApiRouteFilterToPredicateConverter>>() {}));
  }

  private ApiGatewayConfigServiceBlockingStub buildGatewayConfigServiceClient(
      final GatewayConfigServiceCacheConfig config) {
    return ApiGatewayConfigServiceGrpc.newBlockingStub(config.getChannel())
        .withCallCredentials(config.getCallCredentials());
  }
}
