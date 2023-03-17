package ai.traceable.api.gateway.config.service.filter;

import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import java.util.function.Predicate;

public class OrgIdFilterToPredicateConverter implements ApiRouteFilterToPredicateConverter {

  @Override
  public Predicate<ApiRoute> convert(final ApiRouteFilter filter) {
    return route -> filter.getOrgIds().getOrgIdList().contains(route.getMetadata().getOrgId());
  }
}
