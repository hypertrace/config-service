package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AnomalyConfigScopeUtilsTest {
  private final AnomalyConfigScopeUtils scopeMatcher = new AnomalyConfigScopeUtils();

  @Test
  void test() {
    AnomalyCustomerScope customerScope =
        AnomalyCustomerScope.newBuilder().getDefaultInstanceForType();
    AnomalyEnvironmentScope environmentScope =
        AnomalyEnvironmentScope.newBuilder().setEnvironmentId("env_id").build();
    AnomalyServiceScope serviceScope = AnomalyServiceScope.newBuilder().setId("service_id").build();
    AnomalyApiScope apiScope =
        AnomalyApiScope.newBuilder().setServiceScope(serviceScope).setId("api_id").build();
    AnomalyParamScope paramScope =
        AnomalyParamScope.newBuilder().setApiScope(apiScope).setParamName("param_name").build();

    AnomalyConfigScope customerConfigScope =
        AnomalyConfigScope.newBuilder().setCustomerScope(customerScope).build();
    AnomalyConfigScope environmentConfigScope =
        AnomalyConfigScope.newBuilder().setEnvironmentScope(environmentScope).build();
    AnomalyConfigScope serviceConfigScope =
        AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();
    AnomalyConfigScope apiConfigScope =
        AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();
    AnomalyConfigScope paramConfigScope =
        AnomalyConfigScope.newBuilder().setParamScope(paramScope).build();

    AnomalyCustomerScope otherCustomerScope =
        AnomalyCustomerScope.newBuilder().getDefaultInstanceForType();
    AnomalyEnvironmentScope otherEnvironmentScope =
        AnomalyEnvironmentScope.newBuilder().setEnvironmentId("other_env_id").build();
    AnomalyServiceScope otherServiceScope =
        AnomalyServiceScope.newBuilder().setId("other_service_id").build();
    AnomalyApiScope otherApiScope =
        AnomalyApiScope.newBuilder()
            .setServiceScope(otherServiceScope)
            .setId("other_api_id")
            .build();
    AnomalyParamScope otherParamScope =
        AnomalyParamScope.newBuilder()
            .setApiScope(otherApiScope)
            .setParamName("other_param_name")
            .build();

    AnomalyConfigScope otherCustomerConfigScope =
        AnomalyConfigScope.newBuilder().setCustomerScope(otherCustomerScope).build();
    AnomalyConfigScope otherEnvironmentConfigScope =
        AnomalyConfigScope.newBuilder().setEnvironmentScope(otherEnvironmentScope).build();
    AnomalyConfigScope otherServiceConfigScope =
        AnomalyConfigScope.newBuilder().setServiceScope(otherServiceScope).build();
    AnomalyConfigScope otherApiConfigScope =
        AnomalyConfigScope.newBuilder().setApiScope(otherApiScope).build();
    AnomalyConfigScope otherParamConfigScope =
        AnomalyConfigScope.newBuilder().setParamScope(otherParamScope).build();

    Assertions.assertTrue(scopeMatcher.isParentScope(customerConfigScope, customerConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(environmentConfigScope, customerConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(serviceConfigScope, customerConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(apiConfigScope, customerConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(paramConfigScope, customerConfigScope));

    Assertions.assertTrue(scopeMatcher.isParentScope(customerConfigScope, serviceConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(serviceConfigScope, serviceConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherServiceConfigScope, serviceConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(environmentConfigScope, serviceConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(apiConfigScope, serviceConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(paramConfigScope, serviceConfigScope));

    Assertions.assertTrue(scopeMatcher.isParentScope(customerConfigScope, environmentConfigScope));
    Assertions.assertTrue(
        scopeMatcher.isParentScope(environmentConfigScope, environmentConfigScope));
    Assertions.assertFalse(
        scopeMatcher.isParentScope(otherEnvironmentConfigScope, environmentConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(serviceConfigScope, environmentConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(apiConfigScope, environmentConfigScope));

    Assertions.assertTrue(scopeMatcher.isParentScope(customerConfigScope, apiConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(serviceConfigScope, apiConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherServiceConfigScope, apiConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(environmentConfigScope, apiConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(apiConfigScope, apiConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherApiConfigScope, apiConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(paramConfigScope, apiConfigScope));

    Assertions.assertTrue(scopeMatcher.isParentScope(customerConfigScope, paramConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(serviceConfigScope, paramConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(environmentConfigScope, paramConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherServiceConfigScope, paramConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(apiConfigScope, paramConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherApiConfigScope, paramConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(paramConfigScope, paramConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherParamConfigScope, paramConfigScope));
  }
}
