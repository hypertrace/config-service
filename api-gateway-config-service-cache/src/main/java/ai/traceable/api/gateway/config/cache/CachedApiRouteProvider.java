package ai.traceable.api.gateway.config.cache;

import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

@SuppressWarnings("unused")
public interface CachedApiRouteProvider {

  static CachedApiRouteProvider of(final GatewayConfigServiceCacheConfig config) {
    return new DefaultCachedApiRouteProvider(config);
  }

  List<ApiRoute> getRoutes(final ApiRouteFilter filter, final RequestContext context);
}
