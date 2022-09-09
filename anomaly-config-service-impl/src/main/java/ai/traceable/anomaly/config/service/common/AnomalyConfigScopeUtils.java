package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import java.util.ArrayList;
import java.util.List;
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
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", configScope.getScopeCase()));
    }
    return contextsWithIncreasingPriority;
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
