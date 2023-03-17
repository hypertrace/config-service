package ai.traceable.api.gateway.config.service.filter;

import ai.traceable.api.gateway.config.service.v1.ApiRoute;
import ai.traceable.api.gateway.config.service.v1.ApiRouteFilter;
import java.util.function.Predicate;

public class ApiInfoFilterToPredicateConverter implements ApiRouteFilterToPredicateConverter {
  private static final String API_SEGMENT_SEPARATOR = "/";

  @Override
  public Predicate<ApiRoute> convert(final ApiRouteFilter filter) {
    return areMethodsMatching(filter)
        .and(areApiPathSegmentsAPrefixOfRoutePathSegments(filter))
        .and(areServicesMatching(filter));
  }

  private Predicate<ApiRoute> areMethodsMatching(final ApiRouteFilter filter) {
    if (!filter.getApiInfo().hasHttpMethod()) {
      return API_ROUTE_TAUTOLOGY;
    }

    final Predicate<ApiRoute> isMethodInRouteBlank =
        route -> route.getInfo().getHttpMethod().isBlank();
    final Predicate<ApiRoute> isMethodMatchingCaseInsensitively =
        route ->
            filter.getApiInfo().getHttpMethod().equalsIgnoreCase(route.getInfo().getHttpMethod());
    return isMethodInRouteBlank.or(isMethodMatchingCaseInsensitively);
  }

  private Predicate<ApiRoute> areApiPathSegmentsAPrefixOfRoutePathSegments(
      final ApiRouteFilter filter) {
    if (!filter.getApiInfo().hasPath()) {
      return API_ROUTE_TAUTOLOGY;
    }

    final String slashSuffixedApiPath = suffixSlashIfAbsent(filter.getApiInfo().getPath());
    return route -> slashSuffixedApiPath.startsWith(suffixSlashIfAbsent(route.getInfo().getPath()));
  }

  private Predicate<ApiRoute> areServicesMatching(final ApiRouteFilter filter) {
    if (!filter.getApiInfo().hasServiceName()) {
      return API_ROUTE_TAUTOLOGY;
    }

    final Predicate<ApiRoute> isServiceNameInRouteBlank =
        route -> route.getInfo().getServiceName().isBlank();
    final Predicate<ApiRoute> isServiceNameMatching =
        route -> filter.getApiInfo().getServiceName().equals(route.getInfo().getServiceName());
    return isServiceNameInRouteBlank.or(isServiceNameMatching);
  }

  private String suffixSlashIfAbsent(final String original) {
    if (original.isBlank() || original.endsWith(API_SEGMENT_SEPARATOR)) {
      return original;
    }

    return original + API_SEGMENT_SEPARATOR;
  }
}
