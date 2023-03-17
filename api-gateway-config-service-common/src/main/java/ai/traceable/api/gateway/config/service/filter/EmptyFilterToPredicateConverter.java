package ai.traceable.api.gateway.config.service.filter;

import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import java.util.function.Predicate;

public class EmptyFilterToPredicateConverter implements ApiRouteFilterToPredicateConverter {

  @Override
  public Predicate<ApiRoute> convert(final ApiRouteFilter filter) {
    return API_ROUTE_TAUTOLOGY;
  }
}
