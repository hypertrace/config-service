package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyBackendApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyBackendScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AnomalyConfigScopeUtils {

  private static final AnomalyConfigScope CUSTOMER_CONFIG_SCOPE =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();

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
}
