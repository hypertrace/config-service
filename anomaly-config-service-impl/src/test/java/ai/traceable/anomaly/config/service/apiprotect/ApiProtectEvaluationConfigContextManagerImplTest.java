package ai.traceable.anomaly.config.service.apiprotect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.apiprotect.protection.engine.ApiProtectEvaluationConfigContextManagerImpl;
import ai.traceable.anomaly.config.service.apiprotect.protection.engine.cache.ApiProtectConfigContextProvider;
import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.apiprotect.GetApiProtectEvaluationConfigContextRequest;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionConfigContext;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionRuleConfig;
import ai.traceable.protection.engine.config.apiprotect.v1.ApiProtectionRulesContext;
import ai.traceable.protection.processing.common.v1.CustomerScope;
import ai.traceable.protection.processing.common.v1.Entity;
import ai.traceable.protection.processing.common.v1.EntityScope;
import ai.traceable.protection.processing.common.v1.EntityType;
import ai.traceable.protection.processing.common.v1.Scope;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiProtectEvaluationConfigContextManagerImplTest {

  private static final String TENANT_ID = "test-tenant";
  private static final String API_ID = "test-api";
  private static final String SERVICE_ID = "test-service";
  private static final String ENVIRONMENT_ID = "test-env";
  private static final ScopeContext API_SCOPE_CONTEXT =
      ScopeContext.newBuilder()
          .addScopes(
              Scope.newBuilder()
                  .setEntityScope(
                      EntityScope.newBuilder()
                          .setEntityType(EntityType.ENTITY_TYPE_API)
                          .addEntities(Entity.newBuilder().setId(API_ID).setName(API_ID).build())))
          .addScopes(
              Scope.newBuilder()
                  .setEntityScope(
                      EntityScope.newBuilder()
                          .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                          .addEntities(
                              Entity.newBuilder().setId(SERVICE_ID).setName(SERVICE_ID).build())))
          .addScopes(
              Scope.newBuilder()
                  .setEntityScope(
                      EntityScope.newBuilder()
                          .setEntityType(EntityType.ENTITY_TYPE_ENVIRONMENT)
                          .addEntities(
                              Entity.newBuilder()
                                  .setId(ENVIRONMENT_ID)
                                  .setName(ENVIRONMENT_ID)
                                  .build())))
          .addScopes(Scope.newBuilder().setCustomerScope(CustomerScope.getDefaultInstance()))
          .build();

  private ApiProtectEvaluationConfigContextManagerImpl configContextManager;
  private RequestContext requestContext;
  private FeatureCachingClient featureCachingClient;
  private ApiProtectConfigContextProvider configContextProvider;

  @BeforeEach
  void setUp() {
    featureCachingClient = mock(FeatureCachingClient.class);
    configContextProvider = mock(ApiProtectConfigContextProvider.class);
    requestContext = RequestContext.forTenantId(TENANT_ID);

    configContextManager =
        new ApiProtectEvaluationConfigContextManagerImpl(
            featureCachingClient, configContextProvider, configContextProvider);
  }

  @Test
  void testFeatureFlagDisabled_ReturnsDefaultInstance() {
    when(featureCachingClient.isProtectionEngineApiProtectEnabledForTenant(any()))
        .thenReturn(false);
    GetApiProtectEvaluationConfigContextRequest request =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    ApiProtectionConfigContext result =
        configContextManager.getApiProtectEvaluationConfigContext(requestContext, request);

    assertEquals(ApiProtectionConfigContext.getDefaultInstance(), result);
    verify(configContextProvider, times(0)).getApiProtectionConfigContext(any(), any());
  }

  @Test
  void testFeatureFlagEnabled_DelegatesToProvider() {
    when(featureCachingClient.isProtectionEngineApiProtectEnabledForTenant(any())).thenReturn(true);

    ApiProtectionConfigContext expectedContext =
        ApiProtectionConfigContext.newBuilder()
            .addRuleContexts(
                ApiProtectionRulesContext.newBuilder()
                    .setScopeContext(API_SCOPE_CONTEXT)
                    .addRuleConfigs(
                        ApiProtectionRuleConfig.newBuilder().setRuleId("test-rule").build())
                    .build())
            .build();
    GetApiProtectEvaluationConfigContextRequest request =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .build();
    when(configContextProvider.getApiProtectionConfigContext(requestContext, request))
        .thenReturn(expectedContext);
    ApiProtectionConfigContext result =
        configContextManager.getApiProtectEvaluationConfigContext(requestContext, request);

    assertEquals(expectedContext, result);
    verify(configContextProvider, times(1)).getApiProtectionConfigContext(requestContext, request);
  }

  @Test
  void testMultipleCallsDelegateToProvider() {
    when(featureCachingClient.isProtectionEngineApiProtectEnabledForTenant(any())).thenReturn(true);
    ApiProtectionConfigContext expectedContext = ApiProtectionConfigContext.newBuilder().build();
    GetApiProtectEvaluationConfigContextRequest request =
        GetApiProtectEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .build();
    when(configContextProvider.getApiProtectionConfigContext(requestContext, request))
        .thenReturn(expectedContext);
    configContextManager.getApiProtectEvaluationConfigContext(requestContext, request);
    configContextManager.getApiProtectEvaluationConfigContext(requestContext, request);
    verify(configContextProvider, times(2)).getApiProtectionConfigContext(requestContext, request);
  }
}
