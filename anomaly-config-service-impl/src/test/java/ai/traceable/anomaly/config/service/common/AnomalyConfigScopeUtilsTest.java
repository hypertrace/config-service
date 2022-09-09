package ai.traceable.anomaly.config.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AnomalyConfigScopeUtilsTest {
  private final AnomalyConfigScopeUtils scopeMatcher = new AnomalyConfigScopeUtils();
  private final String TENANT_ID = "tenantId";
  private final String ENV_ID = "envId";
  private final String SERVICE_ID = "serviceId";
  private final String API_ID = "apiId";
  private final String PARAM_NAME = "paramName";

  private AnomalyConfigScope customerConfigScope;
  private AnomalyConfigScope environmentConfigScope;
  private AnomalyConfigScope serviceConfigScope;
  private AnomalyConfigScope serviceWithEnvConfigScope;
  private AnomalyConfigScope apiConfigScope;
  private AnomalyConfigScope apiWithEnvConfigScope;
  private AnomalyConfigScope paramConfigScope;
  private AnomalyConfigScope paramWithEnvConfigScope;

  @BeforeEach
  public void setup() {
    AnomalyCustomerScope customerScope =
        AnomalyCustomerScope.newBuilder().getDefaultInstanceForType();
    AnomalyEnvironmentScope environmentScope =
        AnomalyEnvironmentScope.newBuilder().setEnvironmentId(ENV_ID).build();
    AnomalyServiceScope serviceScope = AnomalyServiceScope.newBuilder().setId(SERVICE_ID).build();
    AnomalyServiceScope serviceWithEnvScope =
        AnomalyServiceScope.newBuilder()
            .setId(SERVICE_ID)
            .setEnvironmentScope(environmentScope)
            .build();
    AnomalyApiScope apiScope =
        AnomalyApiScope.newBuilder().setServiceScope(serviceScope).setId(API_ID).build();
    AnomalyApiScope apiWithEnvScope =
        AnomalyApiScope.newBuilder().setServiceScope(serviceWithEnvScope).setId(API_ID).build();
    AnomalyParamScope paramScope =
        AnomalyParamScope.newBuilder().setApiScope(apiScope).setParamName(PARAM_NAME).build();
    AnomalyParamScope paramWithEnvScope =
        AnomalyParamScope.newBuilder()
            .setApiScope(apiWithEnvScope)
            .setParamName(PARAM_NAME)
            .build();

    customerConfigScope = AnomalyConfigScope.newBuilder().setCustomerScope(customerScope).build();
    environmentConfigScope =
        AnomalyConfigScope.newBuilder().setEnvironmentScope(environmentScope).build();
    serviceConfigScope = AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();
    serviceWithEnvConfigScope =
        AnomalyConfigScope.newBuilder().setServiceScope(serviceWithEnvScope).build();
    apiConfigScope = AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();
    apiWithEnvConfigScope = AnomalyConfigScope.newBuilder().setApiScope(apiWithEnvScope).build();
    paramConfigScope = AnomalyConfigScope.newBuilder().setParamScope(paramScope).build();
    paramWithEnvConfigScope =
        AnomalyConfigScope.newBuilder().setParamScope(paramWithEnvScope).build();
  }

  @Test
  void testParentScope() {
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
    Assertions.assertTrue(
        scopeMatcher.isParentScope(environmentConfigScope, serviceWithEnvConfigScope));
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
    Assertions.assertTrue(
        scopeMatcher.isParentScope(environmentConfigScope, apiWithEnvConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(apiConfigScope, apiConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherApiConfigScope, apiConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(paramConfigScope, apiConfigScope));

    Assertions.assertTrue(scopeMatcher.isParentScope(customerConfigScope, paramConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(serviceConfigScope, paramConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(environmentConfigScope, paramConfigScope));
    Assertions.assertTrue(
        scopeMatcher.isParentScope(environmentConfigScope, paramWithEnvConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherServiceConfigScope, paramConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(apiConfigScope, paramConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherApiConfigScope, paramConfigScope));
    Assertions.assertTrue(scopeMatcher.isParentScope(paramConfigScope, paramConfigScope));
    Assertions.assertFalse(scopeMatcher.isParentScope(otherParamConfigScope, paramConfigScope));
  }

  @Test
  public void testGetContextsWithIncreasingPriority() {
    assertEquals(
        List.of(TENANT_ID),
        scopeMatcher.getContextsWithIncreasingPriority(TENANT_ID, customerConfigScope));
    assertEquals(
        List.of(TENANT_ID, ENV_ID),
        scopeMatcher.getContextsWithIncreasingPriority(TENANT_ID, environmentConfigScope));
    assertEquals(
        List.of(TENANT_ID, SERVICE_ID),
        scopeMatcher.getContextsWithIncreasingPriority(TENANT_ID, serviceConfigScope));
    assertEquals(
        List.of(TENANT_ID, ENV_ID, SERVICE_ID),
        scopeMatcher.getContextsWithIncreasingPriority(TENANT_ID, serviceWithEnvConfigScope));
    assertEquals(
        List.of(TENANT_ID, SERVICE_ID, API_ID),
        scopeMatcher.getContextsWithIncreasingPriority(TENANT_ID, apiConfigScope));
    assertEquals(
        List.of(TENANT_ID, ENV_ID, SERVICE_ID, API_ID),
        scopeMatcher.getContextsWithIncreasingPriority(TENANT_ID, apiWithEnvConfigScope));
  }
}
