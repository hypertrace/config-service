package ai.traceable.api.gateway.config.service.filter;

import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import java.util.function.Predicate;

public interface ApiRouteFilterToPredicateConverter {
  Predicate<ApiRoute> API_ROUTE_TAUTOLOGY = route -> true;

  Predicate<ApiRoute> convert(final ApiRouteFilter filter);
}
