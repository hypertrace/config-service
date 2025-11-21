package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyBackendApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyBackendScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope.ScopeCase;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider.ApiIdentifierEntity;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.protection.processing.common.v1.CustomerScope;
import ai.traceable.protection.processing.common.v1.Entity;
import ai.traceable.protection.processing.common.v1.EntityScope;
import ai.traceable.protection.processing.common.v1.EntityType;
import ai.traceable.protection.processing.common.v1.Scope;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyConfigScopeUtils {

  private static final Map<ScopeCase, Integer> SCOPE_ORDER =
      Map.of(
          AnomalyConfigScope.ScopeCase.API_SCOPE,
          1,
          AnomalyConfigScope.ScopeCase.SERVICE_SCOPE,
          2,
          AnomalyConfigScope.ScopeCase.ENVIRONMENT_SCOPE,
          3,
          AnomalyConfigScope.ScopeCase.CUSTOMER_SCOPE,
          4);

  private static final AnomalyConfigScope CUSTOMER_CONFIG_SCOPE =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();

  public static final Comparator<AnomalyConfigScope> ANOMALY_CONFIG_SCOPE_COMPARATOR =
      Comparator.comparing(
              (AnomalyConfigScope scope) ->
                  SCOPE_ORDER.getOrDefault(scope.getScopeCase(), Integer.MAX_VALUE))
          .thenComparing(
              scope -> {
                switch (scope.getScopeCase()) {
                  case API_SCOPE:
                    return scope.getApiScope().getId();
                  case SERVICE_SCOPE:
                    return scope.getServiceScope().getId();
                  case ENVIRONMENT_SCOPE:
                    return scope.getEnvironmentScope().getEnvironmentId();
                  default:
                    return "";
                }
              });

  public static AnomalyConfigScope getDefaultCustomerConfigScope() {
    return CUSTOMER_CONFIG_SCOPE;
  }

  /**
   * @param configScopeToCheck - config scope to be checked if it's parent scope of the required
   *     scope.
   * @param requiredConfigScope - config scope which is required
   * @return - true if configScopeToCheck is parent(or same) scope of requiredConfigScope.
   */
  public boolean isParentScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (requiredConfigScope.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return isParentOfCustomerScope(configScopeToCheck, requiredConfigScope);
      case ENVIRONMENT_SCOPE:
        return isParentOfEnvironmentScope(configScopeToCheck, requiredConfigScope);
      case SERVICE_SCOPE:
        return isParentOfServiceScope(configScopeToCheck, requiredConfigScope);
      case API_SCOPE:
        return isParentOfApiScope(configScopeToCheck, requiredConfigScope);
      case PARAM_SCOPE:
        return isParentOfParamScope(configScopeToCheck, requiredConfigScope);
      case BACKEND_SCOPE:
        return isParentOfBackendScope(configScopeToCheck, requiredConfigScope);
      case BACKEND_API_SCOPE:
        return isParentOfBackendApiScope(configScopeToCheck, requiredConfigScope);
      default:
        return false;
    }
  }

  public String getContextFromAnomalyConfigScope(
      String tenantId, AnomalyConfigScope anomalyConfigScope) {
    String context;
    switch (anomalyConfigScope.getScopeCase()) {
      case CUSTOMER_SCOPE:
        context = tenantId;
        break;
      case ENVIRONMENT_SCOPE:
        context = anomalyConfigScope.getEnvironmentScope().getEnvironmentId();
        break;
      case SERVICE_SCOPE:
        context = anomalyConfigScope.getServiceScope().getId();
        break;
      case API_SCOPE:
        context = anomalyConfigScope.getApiScope().getId();
        break;
      case BACKEND_SCOPE:
        context = anomalyConfigScope.getBackendScope().getId();
        break;
      case BACKEND_API_SCOPE:
        context = anomalyConfigScope.getBackendApiScope().getId();
        break;
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", anomalyConfigScope.getScopeCase()));
    }
    return context;
  }

  public String getContextFromAnomalyConfigScope(AnomalyConfigScope anomalyConfigScope) {
    String tenantId =
        RequestContext.CURRENT
            .get()
            .getTenantId()
            .orElseThrow(
                () -> new IllegalArgumentException("Unable to get tenant id from request context"));
    return getContextFromAnomalyConfigScope(tenantId, anomalyConfigScope);
  }

  public List<String> getContextsWithIncreasingPriority(
      String tenantId, AnomalyConfigScope configScope) {
    /*
     * Precedence Order --> apiConfig > serviceConfig > customerConfig > defaultConfig For example, if
     * apiConfig.disabled = true, we use it; if apiConfig.disabled = false, we use
     * serviceConfig.disabled value and so on.. Similarly for all other config values
     * Environment Scope acts out of hierarchy in parallel which resolves only with customer scope
     */
    List<String> contextsWithIncreasingPriority = new ArrayList<>();
    contextsWithIncreasingPriority.add(tenantId);

    switch (configScope.getScopeCase()) {
      case CUSTOMER_SCOPE:
        break;
      case ENVIRONMENT_SCOPE:
        contextsWithIncreasingPriority.add(configScope.getEnvironmentScope().getEnvironmentId());
        break;
      case SERVICE_SCOPE:
        if (configScope.getServiceScope().hasEnvironmentScope()) {
          contextsWithIncreasingPriority.add(
              configScope.getServiceScope().getEnvironmentScope().getEnvironmentId());
        }
        contextsWithIncreasingPriority.add(configScope.getServiceScope().getId());
        break;
      case API_SCOPE:
        if (configScope.getApiScope().getServiceScope().hasEnvironmentScope()) {
          contextsWithIncreasingPriority.add(
              configScope.getApiScope().getServiceScope().getEnvironmentScope().getEnvironmentId());
        }
        contextsWithIncreasingPriority.add(configScope.getApiScope().getServiceScope().getId());
        contextsWithIncreasingPriority.add(configScope.getApiScope().getId());
        break;
      case BACKEND_SCOPE:
        if (configScope.getBackendScope().hasEnvironmentScope()) {
          contextsWithIncreasingPriority.add(
              configScope.getBackendScope().getEnvironmentScope().getEnvironmentId());
        }
        contextsWithIncreasingPriority.add(configScope.getBackendScope().getId());
        break;
      case BACKEND_API_SCOPE:
        if (configScope.getBackendApiScope().getBackendScope().hasEnvironmentScope()) {
          contextsWithIncreasingPriority.add(
              configScope
                  .getBackendApiScope()
                  .getBackendScope()
                  .getEnvironmentScope()
                  .getEnvironmentId());
        }
        contextsWithIncreasingPriority.add(
            configScope.getBackendApiScope().getBackendScope().getId());
        contextsWithIncreasingPriority.add(configScope.getBackendApiScope().getId());
        break;
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", configScope.getScopeCase()));
    }
    return contextsWithIncreasingPriority;
  }

  public <T> Map<String, T> filterConfigMap(
      Map<String, T> originalMap,
      List<AnomalyConfigScope> applicableScopesList,
      Function<T, AnomalyConfigScope> scopeExtractor) {
    if (applicableScopesList.isEmpty()) {
      return originalMap;
    }

    Multimap<AnomalyConfigScope.ScopeCase, String> scopeCaseToApplicableIdsMap =
        getScopeCaseToApplicableIdsMultimap(applicableScopesList);

    return originalMap.entrySet().stream()
        .filter(
            entry ->
                isConfigMatchesApplicableScope(
                    scopeExtractor.apply(entry.getValue()), scopeCaseToApplicableIdsMap))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private boolean isConfigMatchesApplicableScope(
      AnomalyConfigScope configScope,
      Multimap<AnomalyConfigScope.ScopeCase, String> scopeCaseToApplicableIdsMap) {

    return getScopeCaseToIdMap(configScope).entrySet().stream()
        .anyMatch(
            entry -> scopeCaseToApplicableIdsMap.containsEntry(entry.getKey(), entry.getValue()));
  }

  private Multimap<AnomalyConfigScope.ScopeCase, String> getScopeCaseToApplicableIdsMultimap(
      List<AnomalyConfigScope> anomalyConfigScopes) {

    Multimap<AnomalyConfigScope.ScopeCase, String> multimap = HashMultimap.create();

    for (AnomalyConfigScope anomalyConfigScope : anomalyConfigScopes) {
      getScopeId(anomalyConfigScope)
          .ifPresent(id -> multimap.put(anomalyConfigScope.getScopeCase(), id));
    }

    return Multimaps.unmodifiableMultimap(multimap);
  }

  private Map<AnomalyConfigScope.ScopeCase, String> getScopeCaseToIdMap(
      AnomalyConfigScope anomalyConfigScope) {
    switch (anomalyConfigScope.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        return buildForEnvScope(anomalyConfigScope.getEnvironmentScope());
      case SERVICE_SCOPE:
        return buildForServiceScope(anomalyConfigScope.getServiceScope());
      case API_SCOPE:
        return buildForApiScope(anomalyConfigScope.getApiScope());
      case BACKEND_SCOPE:
        return buildForBackendScope(anomalyConfigScope.getBackendScope());
      case BACKEND_API_SCOPE:
        return buildForBackendApiScope(anomalyConfigScope.getBackendApiScope());
      default:
        return Collections.emptyMap();
    }
  }

  private ImmutableMap<AnomalyConfigScope.ScopeCase, String> buildForEnvScope(
      AnomalyEnvironmentScope anomalyEnvironmentScope) {
    return ImmutableMap.<AnomalyConfigScope.ScopeCase, String>builder()
        .put(
            AnomalyConfigScope.ScopeCase.ENVIRONMENT_SCOPE,
            anomalyEnvironmentScope.getEnvironmentId())
        .build();
  }

  private ImmutableMap<AnomalyConfigScope.ScopeCase, String> buildForServiceScope(
      AnomalyServiceScope anomalyServiceScope) {
    return ImmutableMap.<AnomalyConfigScope.ScopeCase, String>builder()
        .put(AnomalyConfigScope.ScopeCase.SERVICE_SCOPE, anomalyServiceScope.getId())
        .putAll(buildForEnvScope(anomalyServiceScope.getEnvironmentScope()))
        .build();
  }

  private ImmutableMap<AnomalyConfigScope.ScopeCase, String> buildForApiScope(
      AnomalyApiScope anomalyApiScope) {
    return ImmutableMap.<AnomalyConfigScope.ScopeCase, String>builder()
        .put(AnomalyConfigScope.ScopeCase.API_SCOPE, anomalyApiScope.getId())
        .putAll(buildForServiceScope(anomalyApiScope.getServiceScope()))
        .build();
  }

  private ImmutableMap<AnomalyConfigScope.ScopeCase, String> buildForBackendScope(
      AnomalyBackendScope anomalyBackendScope) {
    return ImmutableMap.<AnomalyConfigScope.ScopeCase, String>builder()
        .put(AnomalyConfigScope.ScopeCase.BACKEND_SCOPE, anomalyBackendScope.getId())
        .putAll(buildForEnvScope(anomalyBackendScope.getEnvironmentScope()))
        .build();
  }

  private ImmutableMap<AnomalyConfigScope.ScopeCase, String> buildForBackendApiScope(
      AnomalyBackendApiScope anomalyBackendApiScope) {
    return ImmutableMap.<AnomalyConfigScope.ScopeCase, String>builder()
        .put(AnomalyConfigScope.ScopeCase.BACKEND_API_SCOPE, anomalyBackendApiScope.getId())
        .putAll(buildForBackendScope(anomalyBackendApiScope.getBackendScope()))
        .build();
  }

  private Optional<String> getScopeId(AnomalyConfigScope anomalyConfigScope) {
    switch (anomalyConfigScope.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        return Optional.of(anomalyConfigScope.getEnvironmentScope().getEnvironmentId());
      case SERVICE_SCOPE:
        return Optional.of(anomalyConfigScope.getServiceScope().getId());
      case API_SCOPE:
        return Optional.of(anomalyConfigScope.getApiScope().getId());
      case BACKEND_SCOPE:
        return Optional.of(anomalyConfigScope.getBackendScope().getId());
      case BACKEND_API_SCOPE:
        return Optional.of(anomalyConfigScope.getBackendApiScope().getId());
      default:
        return Optional.empty();
    }
  }

  private boolean isParentOfCustomerScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (configScopeToCheck.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return configScopeToCheck.getCustomerScope().equals(requiredConfigScope.getCustomerScope());
      default:
        return false;
    }
  }

  private boolean isParentOfEnvironmentScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (configScopeToCheck.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return true;
      case ENVIRONMENT_SCOPE:
        return requiredConfigScope
            .getEnvironmentScope()
            .equals(configScopeToCheck.getEnvironmentScope());
      default:
        return false;
    }
  }

  private boolean isParentOfServiceScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (configScopeToCheck.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return true;
      case ENVIRONMENT_SCOPE:
        return requiredConfigScope
            .getServiceScope()
            .getEnvironmentScope()
            .equals(configScopeToCheck.getEnvironmentScope());
      case SERVICE_SCOPE:
        return requiredConfigScope.getServiceScope().equals(configScopeToCheck.getServiceScope());
      default:
        return false;
    }
  }

  private boolean isParentOfBackendScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (configScopeToCheck.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return true;
      case ENVIRONMENT_SCOPE:
        return requiredConfigScope
            .getBackendScope()
            .getEnvironmentScope()
            .equals(configScopeToCheck.getEnvironmentScope());
      case BACKEND_SCOPE:
        return requiredConfigScope.getBackendScope().equals(configScopeToCheck.getBackendScope());
      default:
        return false;
    }
  }

  private boolean isParentOfApiScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (configScopeToCheck.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return true;
      case ENVIRONMENT_SCOPE:
        return requiredConfigScope
            .getApiScope()
            .getServiceScope()
            .getEnvironmentScope()
            .equals(configScopeToCheck.getEnvironmentScope());
      case SERVICE_SCOPE:
        return requiredConfigScope
            .getApiScope()
            .getServiceScope()
            .equals(configScopeToCheck.getServiceScope());
      case API_SCOPE:
        return requiredConfigScope.getApiScope().equals(configScopeToCheck.getApiScope());
      default:
        return false;
    }
  }

  private boolean isParentOfBackendApiScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (configScopeToCheck.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return true;
      case ENVIRONMENT_SCOPE:
        return requiredConfigScope
            .getBackendApiScope()
            .getBackendScope()
            .getEnvironmentScope()
            .equals(configScopeToCheck.getEnvironmentScope());
      case BACKEND_SCOPE:
        return requiredConfigScope
            .getBackendApiScope()
            .getBackendScope()
            .equals(configScopeToCheck.getBackendScope());
      case BACKEND_API_SCOPE:
        return requiredConfigScope
            .getBackendApiScope()
            .equals(configScopeToCheck.getBackendApiScope());
      default:
        return false;
    }
  }

  private boolean isParentOfParamScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (configScopeToCheck.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return true;
      case ENVIRONMENT_SCOPE:
        return requiredConfigScope
            .getParamScope()
            .getApiScope()
            .getServiceScope()
            .getEnvironmentScope()
            .equals(configScopeToCheck.getEnvironmentScope());
      case SERVICE_SCOPE:
        return requiredConfigScope
            .getParamScope()
            .getApiScope()
            .getServiceScope()
            .equals(configScopeToCheck.getServiceScope());
      case API_SCOPE:
        return requiredConfigScope
            .getParamScope()
            .getApiScope()
            .equals(configScopeToCheck.getApiScope());
      case PARAM_SCOPE:
        return requiredConfigScope.getParamScope().equals(configScopeToCheck.getParamScope());
      default:
        return false;
    }
  }

  public static List<AnomalyConfigScope> getConfigScopesWithDecreasingPriority(
      AnomalyConfigScope anomalyConfigScope) {
    if (anomalyConfigScope == null) {
      throw new IllegalArgumentException("Scope must be non-null");
    }
    String environmentId;
    String serviceId;
    String apiId;
    switch (anomalyConfigScope.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return Collections.singletonList(getAnomalyConfigCustomerScope());
      case ENVIRONMENT_SCOPE:
        environmentId = anomalyConfigScope.getEnvironmentScope().getEnvironmentId();
        return ImmutableList.of(
            getAnomalyConfigEnvironmentScope(environmentId), getAnomalyConfigCustomerScope());
      case SERVICE_SCOPE:
        environmentId =
            anomalyConfigScope
                .getApiScope()
                .getServiceScope()
                .getEnvironmentScope()
                .getEnvironmentId();
        serviceId = anomalyConfigScope.getApiScope().getServiceScope().getId();
        return ImmutableList.of(
            getAnomalyConfigServiceScope(environmentId, serviceId),
            getAnomalyConfigEnvironmentScope(environmentId),
            getAnomalyConfigCustomerScope());
      case API_SCOPE:
        environmentId =
            anomalyConfigScope
                .getApiScope()
                .getServiceScope()
                .getEnvironmentScope()
                .getEnvironmentId();
        serviceId = anomalyConfigScope.getApiScope().getServiceScope().getId();
        apiId = anomalyConfigScope.getApiScope().getId();
        return ImmutableList.of(
            getAnomalyConfigApiScope(environmentId, serviceId, apiId),
            getAnomalyConfigServiceScope(environmentId, serviceId),
            getAnomalyConfigEnvironmentScope(environmentId),
            getAnomalyConfigCustomerScope());
      default:
        log.debug("Unsupported scope type: {}", anomalyConfigScope.getScopeCase());
        return Collections.singletonList(getAnomalyConfigCustomerScope());
    }
  }

  private static AnomalyConfigScope getAnomalyConfigEnvironmentScope(String environmentId) {
    return AnomalyConfigScope.newBuilder()
        .setEnvironmentScope(AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentId))
        .build();
  }

  private static AnomalyConfigScope getAnomalyConfigServiceScope(
      String environmentId, String serviceId) {
    return AnomalyConfigScope.newBuilder()
        .setServiceScope(
            AnomalyServiceScope.newBuilder()
                .setId(serviceId)
                .setEnvironmentScope(
                    AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentId)))
        .build();
  }

  private static AnomalyConfigScope getAnomalyConfigApiScope(
      String environmentId, String serviceId, String apiId) {
    return AnomalyConfigScope.newBuilder()
        .setApiScope(
            AnomalyApiScope.newBuilder()
                .setId(apiId)
                .setServiceScope(
                    AnomalyServiceScope.newBuilder()
                        .setId(serviceId)
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentId))))
        .build();
  }

  private static AnomalyConfigScope getAnomalyConfigCustomerScope() {
    return AnomalyConfigScope.newBuilder()
        .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
        .build();
  }

  /**
   * Builds a map of AnomalyConfigScope to ScopeContext for the given set of config scopes.
   *
   * @param requestContext the request context
   * @param configScopes the set of anomaly config scopes to build scope contexts for
   * @param cachedApiMappingProvider provider to fetch API entity details
   * @param cachedServiceMappingProvider provider to fetch service entity details
   * @return map of AnomalyConfigScope to ScopeContext
   */
  public static Map<AnomalyConfigScope, ScopeContext> getScopeContextMap(
      RequestContext requestContext,
      Set<AnomalyConfigScope> configScopes,
      CachedApiMappingProvider cachedApiMappingProvider,
      CachedServiceMappingProvider cachedServiceMappingProvider) {
    Set<String> serviceIds = new HashSet<>();
    Set<String> apiIds = new HashSet<>();

    for (AnomalyConfigScope configScope : configScopes) {
      switch (configScope.getScopeCase()) {
        case API_SCOPE:
          apiIds.add(configScope.getApiScope().getId());
          serviceIds.add(configScope.getApiScope().getServiceScope().getId());
          break;
        case SERVICE_SCOPE:
          serviceIds.add(configScope.getServiceScope().getId());
          break;
        case ENVIRONMENT_SCOPE:
        case CUSTOMER_SCOPE:
          break;
        default:
          log.error("Unsupported scope type: {}", configScope.getScopeCase());
          throw new IllegalArgumentException(
              "Unsupported scope type: " + configScope.getScopeCase());
      }
    }

    Map<String, Optional<ApiIdentifierEntity>> apiEntities =
        cachedApiMappingProvider.getApiIdentifierEntities(requestContext, apiIds);
    Map<String, Optional<ServiceIdentifierEntity>> serviceEntities =
        cachedServiceMappingProvider.getServiceIdentifierEntities(requestContext, serviceIds);
    Map<AnomalyConfigScope, ScopeContext> scopeContextMap = new HashMap<>();
    for (AnomalyConfigScope scope : configScopes) {
      switch (scope.getScopeCase()) {
        case CUSTOMER_SCOPE:
          scopeContextMap.put(
              scope,
              ScopeContext.newBuilder()
                  .addScopes(
                      Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
                  .build());
          break;
        case ENVIRONMENT_SCOPE:
          // NOTE: env id & env name are same
          scopeContextMap.put(
              scope,
              ScopeContext.newBuilder()
                  .addScopes(
                      Scope.newBuilder()
                          .setEntityScope(
                              EntityScope.newBuilder()
                                  .setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT)
                                  .addEntities(
                                      Entity.newBuilder()
                                          .setId(scope.getEnvironmentScope().getEnvironmentId())
                                          .setName(scope.getEnvironmentScope().getEnvironmentId())
                                          .build())))
                  .addScopes(
                      Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
                  .build());
          break;
        case SERVICE_SCOPE:
          scopeContextMap.put(
              scope,
              ScopeContext.newBuilder()
                  .addScopes(
                      Scope.newBuilder()
                          .setEntityScope(
                              EntityScope.newBuilder()
                                  .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                  .addEntities(
                                      Entity.newBuilder()
                                          .setId(scope.getServiceScope().getId())
                                          .setName(
                                              serviceEntities
                                                  .get(scope.getServiceScope().getId())
                                                  .map(ServiceIdentifierEntity::getServiceName)
                                                  .orElse(""))
                                          .build())))
                  .addScopes(
                      Scope.newBuilder()
                          .setEntityScope(
                              EntityScope.newBuilder()
                                  .setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT)
                                  .addEntities(
                                      Entity.newBuilder()
                                          .setId(
                                              scope
                                                  .getServiceScope()
                                                  .getEnvironmentScope()
                                                  .getEnvironmentId())
                                          .setName(
                                              scope
                                                  .getServiceScope()
                                                  .getEnvironmentScope()
                                                  .getEnvironmentId())
                                          .build())))
                  .addScopes(
                      Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
                  .build());
          break;
        case API_SCOPE:
          scopeContextMap.put(
              scope,
              ScopeContext.newBuilder()
                  .addScopes(
                      Scope.newBuilder()
                          .setEntityScope(
                              EntityScope.newBuilder()
                                  .setEntityType(EntityType.ENTITY_TYPE_API)
                                  .addEntities(
                                      Entity.newBuilder()
                                          .setId(scope.getApiScope().getId())
                                          .setName(
                                              apiEntities
                                                  .get(scope.getApiScope().getId())
                                                  .map(ApiIdentifierEntity::getApiName)
                                                  .orElse(""))
                                          .build())))
                  .addScopes(
                      Scope.newBuilder()
                          .setEntityScope(
                              EntityScope.newBuilder()
                                  .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                  .addEntities(
                                      Entity.newBuilder()
                                          .setId(scope.getApiScope().getServiceScope().getId())
                                          .setName(
                                              serviceEntities
                                                  .get(
                                                      scope.getApiScope().getServiceScope().getId())
                                                  .map(ServiceIdentifierEntity::getServiceName)
                                                  .orElse(""))
                                          .build())))
                  .addScopes(
                      Scope.newBuilder()
                          .setEntityScope(
                              EntityScope.newBuilder()
                                  .setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT)
                                  .addEntities(
                                      Entity.newBuilder()
                                          .setId(
                                              scope
                                                  .getApiScope()
                                                  .getServiceScope()
                                                  .getEnvironmentScope()
                                                  .getEnvironmentId())
                                          .setName(
                                              scope
                                                  .getApiScope()
                                                  .getServiceScope()
                                                  .getEnvironmentScope()
                                                  .getEnvironmentId())
                                          .build())))
                  .addScopes(
                      Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
                  .build());
          break;
        default:
          log.error("Unsupported scope type: {}", scope.getScopeCase());
          throw new IllegalArgumentException("Unsupported scope type: " + scope.getScopeCase());
      }
    }
    return scopeContextMap;
  }
}
