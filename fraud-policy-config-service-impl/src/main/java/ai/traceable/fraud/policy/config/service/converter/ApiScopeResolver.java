package ai.traceable.fraud.policy.config.service.converter;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition.HttpMethodScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition.ServiceScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition.UrlRegexScope;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
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
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * Resolves AbusePolicy api_scope to EDS scope conditions (url_regex_scope, http_method_scope,
 * service_scope) by delegating entity lookups to {@link CachedApiMappingProvider}.
 *
 * <p>Mirrors the pattern used in the fingerprinting-job's EdgeDecisionRuleConverter.get_rule_scope.
 */
@Slf4j
@Singleton
public class ApiScopeResolver {

  private final CachedApiMappingProvider cachedApiMappingProvider;

  @Inject
  public ApiScopeResolver(CachedApiMappingProvider cachedApiMappingProvider) {
    this.cachedApiMappingProvider = cachedApiMappingProvider;
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
   * Aggregates URL patterns, HTTP methods, and service names from an API details map into the
   * provided output sets. Shared between EDS scope condition building and JEXL scope conversion.
   */
  public static void aggregateApiDetails(
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
}
