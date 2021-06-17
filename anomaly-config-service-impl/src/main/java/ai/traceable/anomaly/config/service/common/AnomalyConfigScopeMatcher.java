package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;

public class AnomalyConfigScopeMatcher {

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

  private boolean isParentOfCustomerScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (configScopeToCheck.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return configScopeToCheck.getCustomerScope().equals(requiredConfigScope.getCustomerScope());
      default:
        return false;
    }
  }

  private boolean isParentOfServiceScope(
      AnomalyConfigScope configScopeToCheck, AnomalyConfigScope requiredConfigScope) {
    switch (configScopeToCheck.getScopeCase()) {
      case CUSTOMER_SCOPE:
        return true;
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
