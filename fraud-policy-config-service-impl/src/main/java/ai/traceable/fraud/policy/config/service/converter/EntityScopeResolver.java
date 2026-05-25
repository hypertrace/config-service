package ai.traceable.fraud.policy.config.service.converter;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition.HttpMethodScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition.ServiceScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition.UrlRegexScope;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.fraud.policy.config.service.v1.AbuseApiScope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * Resolves entity scopes (API IDs, API labels, service IDs) to structured data by delegating
 * lookups to {@link CachedApiMappingProvider} and {@link CachedServiceMappingProvider}.
 *
 * <p>Provides both EDS proto scope condition output (for rule_scope) and raw resolved data (for
 * JEXL match_condition generation by {@link ScopeToJexlConverter}).
 */
@Slf4j
@Singleton
public class EntityScopeResolver {

  private final CachedApiMappingProvider cachedApiMappingProvider;
  private final CachedServiceMappingProvider cachedServiceMappingProvider;

  @Inject
  public EntityScopeResolver(
      CachedApiMappingProvider cachedApiMappingProvider,
      CachedServiceMappingProvider cachedServiceMappingProvider) {
    this.cachedApiMappingProvider = cachedApiMappingProvider;
    this.cachedServiceMappingProvider = cachedServiceMappingProvider;
  }

  /**
   * Resolves the API IDs from an AbuseApiScope, handling both direct api_ids and api_labels.
   *
   * @return resolved set of API IDs, or empty if none
   */
  public Set<String> resolveApiIds(RequestContext requestContext, AbuseApiScope apiScope) {
    if (apiScope == null) {
      return Set.of();
    }

    Set<String> apiIds = new HashSet<>();
    if (apiScope.hasApiIds()) {
      apiIds.addAll(apiScope.getApiIds().getIdsList());
    }
    if (apiScope.hasApiLabels() && !apiScope.getApiLabels().getLabelsList().isEmpty()) {
      try {
        Set<String> labelIds = new HashSet<>(apiScope.getApiLabels().getLabelsList());
        Map<String, Set<ApiIdentifierEntity>> entitiesByLabel =
            cachedApiMappingProvider.getApiIdentifierEntitiesHavingLabels(requestContext, labelIds);
        Set<String> resolvedIds =
            entitiesByLabel.values().stream()
                .flatMap(Set::stream)
                .map(ApiIdentifierEntity::getApiId)
                .collect(Collectors.toSet());
        apiIds.addAll(resolvedIds);
      } catch (Exception e) {
        log.warn("Failed to resolve api_labels to API IDs, skipping api_labels scope", e);
      }
    }
    return apiIds;
  }

  /**
   * Resolves api_scope to EDS scope conditions. For each API ID, fetches resolvedUrlPatterns,
   * httpMethod, and serviceName from the entity service.
   *
   * @return list of scope conditions (url_regex_scope, http_method_scope, service_scope), or empty
   *     if resolution fails or no API scope is present
   */
  public List<EdgeDecisionRuleScopeCondition> resolveApiScope(
      RequestContext requestContext, AbuseApiScope apiScope) {
    List<EdgeDecisionRuleScopeCondition> conditions = new ArrayList<>();

    Set<String> apiIds = resolveApiIds(requestContext, apiScope);
    if (apiIds.isEmpty()) {
      return conditions;
    }

    try {
      Map<String, Optional<ApiIdentifierEntity>> apiDetailsMap =
          cachedApiMappingProvider.getApiIdentifierEntities(requestContext, apiIds);

      Set<String> allUrlRegexes = new HashSet<>();
      Set<String> allHttpMethods = new HashSet<>();
      Set<String> allServiceNames = new HashSet<>();
      aggregateApiDetails(apiDetailsMap, allUrlRegexes, allHttpMethods, allServiceNames);

      if (!allUrlRegexes.isEmpty()) {
        conditions.add(
            EdgeDecisionRuleScopeCondition.newBuilder()
                .setUrlRegexScope(UrlRegexScope.newBuilder().addAllUrlRegexes(allUrlRegexes))
                .build());
      }

      if (!allHttpMethods.isEmpty()) {
        conditions.add(
            EdgeDecisionRuleScopeCondition.newBuilder()
                .setHttpMethodScope(HttpMethodScope.newBuilder().addAllHttpMethods(allHttpMethods))
                .build());
      }

      if (!allServiceNames.isEmpty()) {
        conditions.add(
            EdgeDecisionRuleScopeCondition.newBuilder()
                .setServiceScope(ServiceScope.newBuilder().addAllServiceNames(allServiceNames))
                .build());
      }
    } catch (Exception e) {
      log.warn("Failed to resolve API scope from entity service, skipping api_scope", e);
    }

    return conditions;
  }

  /**
   * Resolves API IDs to their detail components (URL patterns, HTTP methods, service names). Used
   * by {@link ScopeToJexlConverter} for generating JEXL match_condition expressions.
   *
   * @return aggregated details, or empty if resolution fails
   */
  public ApiDetails resolveApiDetails(RequestContext requestContext, Set<String> apiIds) {
    try {
      Map<String, Optional<ApiIdentifierEntity>> apiDetailsMap =
          cachedApiMappingProvider.getApiIdentifierEntities(requestContext, apiIds);

      Set<String> urlRegexes = new HashSet<>();
      Set<String> httpMethods = new HashSet<>();
      Set<String> serviceNames = new HashSet<>();
      aggregateApiDetails(apiDetailsMap, urlRegexes, httpMethods, serviceNames);

      return new ApiDetails(urlRegexes, httpMethods, serviceNames);
    } catch (Exception e) {
      log.warn("Failed to resolve API IDs for match_condition, skipping", e);
      return ApiDetails.EMPTY;
    }
  }

  /**
   * Resolves service IDs to service names via the entity query service. Used by {@link
   * ScopeToJexlConverter} for generating JEXL match_condition expressions.
   *
   * @return resolved service names, or empty list if resolution fails
   */
  public List<String> resolveServiceNames(RequestContext requestContext, Set<String> serviceIds) {
    try {
      Map<String, Optional<ServiceIdentifierEntity>> serviceEntities =
          cachedServiceMappingProvider.getServiceIdentifierEntities(requestContext, serviceIds);

      List<String> serviceNames =
          serviceEntities.values().stream()
              .flatMap(Optional::stream)
              .map(ServiceIdentifierEntity::getServiceName)
              .collect(Collectors.toUnmodifiableList());

      if (serviceNames.isEmpty()) {
        log.warn(
            "Could not resolve any service names for service IDs: {}, skipping scope", serviceIds);
      }
      return serviceNames;
    } catch (Exception e) {
      log.warn("Failed to resolve service IDs to names, skipping", e);
      return List.of();
    }
  }

  /**
   * Aggregates URL patterns, HTTP methods, and service names from an API details map into the
   * provided output sets. Shared between EDS scope condition building and JEXL scope conversion.
   */
  static void aggregateApiDetails(
      Map<String, Optional<ApiIdentifierEntity>> apiDetailsMap,
      Set<String> urlRegexes,
      Set<String> httpMethods,
      Set<String> serviceNames) {
    apiDetailsMap.values().stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(
            details -> {
              urlRegexes.addAll(details.getResolvedUrlPatterns());
              if (!details.getHttpMethod().isEmpty()) {
                httpMethods.add(details.getHttpMethod());
              }
              if (!details.getServiceName().isEmpty()) {
                serviceNames.add(details.getServiceName());
              }
            });
  }

  /** Aggregated API entity details for use in JEXL generation. */
  @Value
  public static class ApiDetails {
    static final ApiDetails EMPTY = new ApiDetails(Set.of(), Set.of(), Set.of());

    Set<String> urlRegexes;
    Set<String> httpMethods;
    Set<String> serviceNames;

    public boolean isEmpty() {
      return urlRegexes.isEmpty() && httpMethods.isEmpty() && serviceNames.isEmpty();
    }
  }
}
