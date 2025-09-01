package ai.traceable.genai.config.service.v1.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.genai.config.service.v1.ChatbotFeatureConfig;
import ai.traceable.genai.config.service.v1.EnvironmentScope;
import ai.traceable.genai.config.service.v1.FeatureLevelConfigUpdate;
import ai.traceable.genai.config.service.v1.GenAiConfig;
import ai.traceable.genai.config.service.v1.GenAiFeatureConfigUpdate;
import ai.traceable.genai.config.service.v1.GenAiScope;
import ai.traceable.genai.config.service.v1.GlobalConfig;
import ai.traceable.genai.config.service.v1.GlobalConfigUpdate;
import ai.traceable.genai.config.service.v1.IssuesSummaryFeatureConfig;
import ai.traceable.genai.config.service.v1.ThreatActivitySummaryFeatureConfig;
import ai.traceable.genai.config.service.v1.UpdateGenAiConfigRequest;
import ai.traceable.genai.config.service.v1.feature.config.ChatbotFeatureConfigHandler;
import ai.traceable.genai.config.service.v1.feature.config.GenAiFeatureConfigHandler;
import ai.traceable.genai.config.service.v1.feature.config.GlobalConfigHandler;
import ai.traceable.genai.config.service.v1.feature.config.IssuesSummaryFeatureConfigHandler;
import ai.traceable.genai.config.service.v1.feature.config.ThreatActivitySummaryFeatureConfigHandler;
import ai.traceable.genai.config.service.v1.genai.config.DefaultGenAiConfigProvider;
import ai.traceable.genai.config.service.v1.genai.config.GenAiConfigHandler;
import ai.traceable.genai.config.service.v1.store.GenAiConfigConverter;
import ai.traceable.genai.config.service.v1.store.GenAiConfigIdGenerator;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenAiConfigManagerImplTest {

  @Mock private DefaultGenAiConfigProvider defaultGenAiConfigProvider;
  @Mock private RequestContext requestContext;

  private GenAiConfigManagerImpl manager;
  private MockGenAiConfigStore genAiConfigStore;

  @BeforeEach
  void setUp() {
    GenAiFeatureConfigHandler<GlobalConfig> globalConfigHandler = new GlobalConfigHandler();
    GenAiFeatureConfigHandler<IssuesSummaryFeatureConfig> issuesHandler =
        new IssuesSummaryFeatureConfigHandler();
    GenAiFeatureConfigHandler<ThreatActivitySummaryFeatureConfig> threatHandler =
        new ThreatActivitySummaryFeatureConfigHandler();
    GenAiFeatureConfigHandler<ChatbotFeatureConfig> chatbotHandler =
        new ChatbotFeatureConfigHandler();
    GenAiConfigHandler genAiConfigHandler =
        new GenAiConfigHandler(
            Set.of(globalConfigHandler, issuesHandler, threatHandler, chatbotHandler));

    ConfigServiceGrpc.ConfigServiceBlockingStub stub =
        mock(ConfigServiceGrpc.ConfigServiceBlockingStub.class);
    ConfigChangeEventGenerator eventGenerator = mock(ConfigChangeEventGenerator.class);
    GenAiConfigConverter converter = mock(GenAiConfigConverter.class);
    GenAiConfigIdGenerator idGenerator = new GenAiConfigIdGenerator(new UuidGenerator());

    genAiConfigStore = new MockGenAiConfigStore(stub, eventGenerator, converter, idGenerator);
    manager =
        new GenAiConfigManagerImpl(
            defaultGenAiConfigProvider, genAiConfigHandler, genAiConfigStore);

    when(requestContext.getTenantId()).thenReturn(Optional.of("test-tenant"));
  }

  @Test
  void test_getGenAiConfig_whenScopeIsDefault() {
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();
    GenAiConfig defaultConfig = createGenAiConfig(true);
    when(defaultGenAiConfigProvider.get()).thenReturn(defaultConfig);

    GenAiConfig result = manager.getGenAiConfig(requestContext, defaultScope);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertTrue(result.hasChatBotFeatureConfig());
    assertTrue(result.getChatBotFeatureConfig().getEnabled());
    assertEquals(defaultScope, result.getScope());
  }

  @Test
  void test_getGenAiConfig_whenScopeHasConfig() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();
    GenAiConfig defaultConfig = createGenAiConfig(false);
    GenAiConfig scopedConfig = createGenAiConfig(true).toBuilder().setScope(scope).build();

    when(defaultGenAiConfigProvider.get()).thenReturn(defaultConfig);
    genAiConfigStore.upsertObject(requestContext, scopedConfig);

    GenAiConfig result = manager.getGenAiConfig(requestContext, scope);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_getGenAiConfig_whenScopeDoesNotHaveConfig() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();
    GenAiConfig defaultConfig = createGenAiConfig(true);

    when(defaultGenAiConfigProvider.get()).thenReturn(defaultConfig);

    GenAiConfig result = manager.getGenAiConfig(requestContext, scope);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertTrue(result.hasChatBotFeatureConfig());
    assertTrue(result.getChatBotFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_whenConfigExists() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    IssuesSummaryFeatureConfig featureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder().setIssuesSummaryFeatureConfig(featureConfig).build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder().setScope(scope).setUpdate(update).build();

    GenAiConfig existingConfig = createGenAiConfig(false).toBuilder().setScope(scope).build();
    when(defaultGenAiConfigProvider.get()).thenReturn(createGenAiConfig(false));
    genAiConfigStore.upsertObject(requestContext, existingConfig);

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_whenConfigDoesNotExist() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    IssuesSummaryFeatureConfig featureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder().setIssuesSummaryFeatureConfig(featureConfig).build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder().setScope(scope).setUpdate(update).build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createGenAiConfig(false));

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_getGenAiConfig_withDefaultAndScopedConfig() {
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();
    GenAiConfig defaultScopeConfig =
        createGenAiConfig(true).toBuilder().setScope(defaultScope).build();
    genAiConfigStore.upsertObject(requestContext, defaultScopeConfig);

    GenAiScope envScope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();
    GenAiConfig envScopeConfig = createGenAiConfig(false).toBuilder().setScope(envScope).build();
    genAiConfigStore.upsertObject(requestContext, envScopeConfig);

    when(defaultGenAiConfigProvider.get()).thenReturn(createGenAiConfig(false));

    GenAiConfig result = manager.getGenAiConfig(requestContext, envScope);
    assertNotNull(result);
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(envScope, result.getScope());

    GenAiConfig defaultResult = manager.getGenAiConfig(requestContext, defaultScope);
    assertNotNull(defaultResult);
    assertTrue(defaultResult.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(defaultScope, defaultResult.getScope());
  }

  @Test
  void test_getGenAiConfig_multipleTenantsWithDifferentConfigs() {
    when(requestContext.getTenantId()).thenReturn(Optional.of("tenant1"));
    GenAiScope scope1 =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();
    GenAiConfig config1 = createGenAiConfig(true).toBuilder().setScope(scope1).build();
    genAiConfigStore.upsertObject(requestContext, config1);

    RequestContext tenant2Context = mock(RequestContext.class);
    when(tenant2Context.getTenantId()).thenReturn(Optional.of("tenant2"));
    GenAiScope scope2 =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();
    GenAiConfig config2 = createGenAiConfig(false).toBuilder().setScope(scope2).build();
    genAiConfigStore.upsertObject(tenant2Context, config2);

    when(defaultGenAiConfigProvider.get()).thenReturn(createGenAiConfig(false));

    GenAiConfig result1 = manager.getGenAiConfig(requestContext, scope1);
    assertNotNull(result1);
    assertTrue(result1.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(scope1, result1.getScope());

    GenAiConfig result2 = manager.getGenAiConfig(tenant2Context, scope2);
    assertNotNull(result2);
    assertFalse(result2.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(scope2, result2.getScope());
  }

  @Test
  void test_getGenAiConfig_multipleScopes() {
    GenAiScope env1Scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();
    GenAiScope env2Scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env2").build())
            .build();
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();

    GenAiConfig env1Config = createGenAiConfig(true).toBuilder().setScope(env1Scope).build();
    GenAiConfig env2Config = createGenAiConfig(false).toBuilder().setScope(env2Scope).build();
    GenAiConfig defaultConfig = createGenAiConfig(true).toBuilder().setScope(defaultScope).build();

    genAiConfigStore.upsertObject(requestContext, env1Config);
    genAiConfigStore.upsertObject(requestContext, env2Config);
    genAiConfigStore.upsertObject(requestContext, defaultConfig);

    when(defaultGenAiConfigProvider.get()).thenReturn(createGenAiConfig(false));

    GenAiConfig result1 = manager.getGenAiConfig(requestContext, env1Scope);
    assertTrue(result1.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(env1Scope, result1.getScope());

    GenAiConfig result2 = manager.getGenAiConfig(requestContext, env2Scope);
    assertFalse(result2.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(env2Scope, result2.getScope());

    GenAiConfig resultDefault = manager.getGenAiConfig(requestContext, defaultScope);
    assertTrue(resultDefault.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(defaultScope, resultDefault.getScope());
  }

  @Test
  void test_updateGenAiConfig_multipleUpdatesForSameScope() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiFeatureConfigUpdate update1 =
        GenAiFeatureConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();
    UpdateGenAiConfigRequest request1 =
        UpdateGenAiConfigRequest.newBuilder().setScope(scope).setUpdate(update1).build();

    GenAiFeatureConfigUpdate update2 =
        GenAiFeatureConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build())
            .build();
    UpdateGenAiConfigRequest request2 =
        UpdateGenAiConfigRequest.newBuilder().setScope(scope).setUpdate(update2).build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createGenAiConfig(false));

    GenAiConfig result1 = manager.updateGenAiConfig(requestContext, scope, request1);
    assertTrue(result1.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(scope, result1.getScope());

    GenAiConfig result2 = manager.updateGenAiConfig(requestContext, scope, request2);
    assertFalse(result2.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(scope, result2.getScope());

    GenAiConfig finalConfig = manager.getGenAiConfig(requestContext, scope);
    assertFalse(finalConfig.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(scope, finalConfig.getScope());
  }

  @Test
  void test_getGenAiConfig_whenDefaultScopeHasConfigButEnvScopeDoesNot() {
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();
    GenAiConfig defaultScopeConfig =
        createGenAiConfig(true).toBuilder().setScope(defaultScope).build();
    genAiConfigStore.upsertObject(requestContext, defaultScopeConfig);

    GenAiScope envScope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createGenAiConfig(false));

    GenAiConfig result = manager.getGenAiConfig(requestContext, envScope);

    assertNotNull(result);
    assertEquals(envScope, result.getScope());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());

    GenAiConfig defaultConfig = manager.getGenAiConfig(requestContext, defaultScope);
    assertTrue(defaultConfig.hasIssuesSummaryFeatureConfig());
    assertTrue(defaultConfig.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(defaultScope, defaultConfig.getScope());
  }

  @Test
  void test_getGenAiConfig_whenDefaultScopeHasConfigButMultipleEnvScopesDont() {
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();
    GenAiConfig defaultScopeConfig =
        createGenAiConfig(true).toBuilder().setScope(defaultScope).build();
    genAiConfigStore.upsertObject(requestContext, defaultScopeConfig);

    GenAiScope env1Scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();
    GenAiScope env2Scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env2").build())
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createGenAiConfig(false));

    GenAiConfig result1 = manager.getGenAiConfig(requestContext, env1Scope);
    assertNotNull(result1);
    assertEquals(env1Scope, result1.getScope());
    assertTrue(result1.hasIssuesSummaryFeatureConfig());
    assertTrue(result1.getIssuesSummaryFeatureConfig().getEnabled());

    GenAiConfig result2 = manager.getGenAiConfig(requestContext, env2Scope);
    assertNotNull(result2);
    assertEquals(env2Scope, result2.getScope());
    assertTrue(result2.hasIssuesSummaryFeatureConfig());
    assertTrue(result2.getIssuesSummaryFeatureConfig().getEnabled());

    GenAiConfig defaultConfig = manager.getGenAiConfig(requestContext, defaultScope);
    assertTrue(defaultConfig.hasIssuesSummaryFeatureConfig());
    assertTrue(defaultConfig.getIssuesSummaryFeatureConfig().getEnabled());
    assertEquals(defaultScope, defaultConfig.getScope());
  }

  @Test
  void test_getGenAiConfig_multipleTenantsWithDefaultScopeOnly() {
    when(requestContext.getTenantId()).thenReturn(Optional.of("tenant1"));
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();
    GenAiConfig tenant1DefaultConfig =
        createGenAiConfig(true).toBuilder().setScope(defaultScope).build();
    genAiConfigStore.upsertObject(requestContext, tenant1DefaultConfig);

    RequestContext tenant2Context = mock(RequestContext.class);
    when(tenant2Context.getTenantId()).thenReturn(Optional.of("tenant2"));
    GenAiConfig tenant2DefaultConfig =
        createGenAiConfig(false).toBuilder().setScope(defaultScope).build();
    genAiConfigStore.upsertObject(tenant2Context, tenant2DefaultConfig);

    when(defaultGenAiConfigProvider.get()).thenReturn(createGenAiConfig(false));

    GenAiScope envScope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig result1 = manager.getGenAiConfig(requestContext, envScope);
    assertNotNull(result1);
    assertEquals(envScope, result1.getScope());
    assertTrue(result1.getIssuesSummaryFeatureConfig().getEnabled());

    GenAiConfig result2 = manager.getGenAiConfig(tenant2Context, envScope);
    assertNotNull(result2);
    assertEquals(envScope, result2.getScope());
    assertFalse(result2.getIssuesSummaryFeatureConfig().getEnabled());
  }

  @Test
  void test_getGenAiConfig_withDefaultDisabledConfig() {
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();
    GenAiConfig defaultConfig = createDefaultDisabledConfig();
    when(defaultGenAiConfigProvider.get()).thenReturn(defaultConfig);

    GenAiConfig result = manager.getGenAiConfig(requestContext, defaultScope);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(defaultScope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_globalConfigOnly() {
    GenAiScope scope = GenAiScope.getDefaultInstance();

    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(true).build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setGlobalConfigUpdate(globalConfigUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_globalConfigDisableFromEnabled() {
    GenAiScope scope = GenAiScope.getDefaultInstance();

    GenAiConfig existingConfig = createGenAiConfig(true).toBuilder().setScope(scope).build();
    genAiConfigStore.upsertObject(requestContext, existingConfig);

    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(false).build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setGlobalConfigUpdate(globalConfigUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_mixedFeatureAndGlobalUpdate() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    IssuesSummaryFeatureConfig featureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate featureUpdate =
        GenAiFeatureConfigUpdate.newBuilder().setIssuesSummaryFeatureConfig(featureConfig).build();
    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(true).build();

    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setUpdate(featureUpdate)
            .setGlobalConfigUpdate(globalConfigUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_getGenAiConfig_hierarchyMerging_globalVsFeatureLevel() {
    GenAiScope defaultScope = GenAiScope.getDefaultInstance();
    GenAiConfig globalScopeConfig =
        createConfigWithGlobalOnly(true).toBuilder().setScope(defaultScope).build();
    genAiConfigStore.upsertObject(requestContext, globalScopeConfig);

    GenAiScope envScope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();
    GenAiConfig envScopeConfig =
        GenAiConfig.newBuilder()
            .setScope(envScope)
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, envScopeConfig);

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.getGenAiConfig(requestContext, envScope);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(envScope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_threatActivitySummaryFeature() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    ThreatActivitySummaryFeatureConfig threatFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder()
            .setThreatActivitySummaryFeatureConfig(threatFeatureConfig)
            .build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder().setScope(scope).setUpdate(update).build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_allFeaturesEnabled() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    ThreatActivitySummaryFeatureConfig threatFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate featureUpdate =
        GenAiFeatureConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfig(issuesFeatureConfig)
            .setThreatActivitySummaryFeatureConfig(threatFeatureConfig)
            .build();
    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(true).build();

    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setUpdate(featureUpdate)
            .setGlobalConfigUpdate(globalConfigUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_getGenAiConfig_partialFeatureOverride() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig partialConfig =
        GenAiConfig.newBuilder()
            .setScope(scope)
            .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(true).build())
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, partialConfig);

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.getGenAiConfig(requestContext, scope);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_backwardCompatibility_oldFeatureConfigUpdate() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    IssuesSummaryFeatureConfig featureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder().setIssuesSummaryFeatureConfig(featureConfig).build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder().setScope(scope).setUpdate(update).build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_newFlow_globalConfigUpdate() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(true).build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setGlobalConfigUpdate(globalConfigUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_mixedFlow_bothUpdateTypes() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    ThreatActivitySummaryFeatureConfig threatFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate featureUpdate =
        GenAiFeatureConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfig(issuesFeatureConfig)
            .setThreatActivitySummaryFeatureConfig(threatFeatureConfig)
            .build();
    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(false).build();

    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setUpdate(featureUpdate)
            .setGlobalConfigUpdate(globalConfigUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_defaultDisabledConfigFlow() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.getGenAiConfig(requestContext, scope);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_getGenAiConfig_globalDisabledIssuesEnabledThreatDisabled() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig storedConfig =
        GenAiConfig.newBuilder()
            .setScope(scope)
            .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(false).build())
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .setThreatActivitySummaryFeatureConfig(
                ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, storedConfig);

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.getGenAiConfig(requestContext, scope);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_newFlowIssuesUpdateWithExistingMixedConfig() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig existingConfig =
        GenAiConfig.newBuilder()
            .setScope(scope)
            .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(true).build())
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build())
            .setThreatActivitySummaryFeatureConfig(
                ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, existingConfig);

    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate update =
        GenAiFeatureConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfig(issuesFeatureConfig)
            .build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder().setScope(scope).setUpdate(update).build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_multipleFeaturesWithGlobalDisabledInStore() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig existingConfig =
        GenAiConfig.newBuilder()
            .setScope(scope)
            .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(false).build())
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build())
            .setThreatActivitySummaryFeatureConfig(
                ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, existingConfig);

    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    ThreatActivitySummaryFeatureConfig threatFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate featureUpdate =
        GenAiFeatureConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfig(issuesFeatureConfig)
            .setThreatActivitySummaryFeatureConfig(threatFeatureConfig)
            .build();

    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder().setScope(scope).setUpdate(featureUpdate).build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_multipleFeaturesWithGlobalEnabledInStore() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig existingConfig =
        GenAiConfig.newBuilder()
            .setScope(scope)
            .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(true).build())
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .setThreatActivitySummaryFeatureConfig(
                ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, existingConfig);

    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build();
    ThreatActivitySummaryFeatureConfig threatFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build();
    GenAiFeatureConfigUpdate featureUpdate =
        GenAiFeatureConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfig(issuesFeatureConfig)
            .setThreatActivitySummaryFeatureConfig(threatFeatureConfig)
            .build();

    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder().setScope(scope).setUpdate(featureUpdate).build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_getGenAiConfig_partialConfigurationsInStore() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig partialConfig =
        GenAiConfig.newBuilder()
            .setScope(scope)
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, partialConfig);

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.getGenAiConfig(requestContext, scope);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_overridePartialExistingConfig() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig existingPartialConfig =
        GenAiConfig.newBuilder()
            .setScope(scope)
            .setThreatActivitySummaryFeatureConfig(
                ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, existingPartialConfig);

    GlobalConfigUpdate globalConfigUpdate =
        GlobalConfigUpdate.newBuilder().setEnabled(true).build();
    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate featureUpdate =
        GenAiFeatureConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfig(issuesFeatureConfig)
            .build();

    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setGlobalConfigUpdate(globalConfigUpdate)
            .setUpdate(featureUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_featureLevelConfigUpdate_issuesOnly() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfigUpdate(issuesFeatureConfig)
            .build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_featureLevelConfigUpdate_threatOnly() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    ThreatActivitySummaryFeatureConfig threatFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setThreatActivitySummaryFeatureConfigUpdate(threatFeatureConfig)
            .build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_featureLevelConfigUpdate_bothFeatures() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    ThreatActivitySummaryFeatureConfig threatFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfigUpdate(issuesFeatureConfig)
            .setThreatActivitySummaryFeatureConfigUpdate(threatFeatureConfig)
            .build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_featureLevelConfigUpdate_withExistingMixedStore() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig existingConfig =
        GenAiConfig.newBuilder()
            .setScope(scope)
            .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(true).build())
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .setThreatActivitySummaryFeatureConfig(
                ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, existingConfig);

    ThreatActivitySummaryFeatureConfig threatFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setThreatActivitySummaryFeatureConfigUpdate(threatFeatureConfig)
            .build();
    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertTrue(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertTrue(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_featureLevelVsDeprecatedFlowPriority() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    IssuesSummaryFeatureConfig deprecatedFlowConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build();
    GenAiFeatureConfigUpdate deprecatedUpdate =
        GenAiFeatureConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfig(deprecatedFlowConfig)
            .build();

    IssuesSummaryFeatureConfig newFlowConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfigUpdate(newFlowConfig)
            .build();

    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setUpdate(deprecatedUpdate)
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertFalse(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  @Test
  void test_updateGenAiConfig_featureLevelConfigUpdate_disableFeatures() {
    GenAiScope scope =
        GenAiScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("env1").build())
            .build();

    GenAiConfig existingConfig =
        GenAiConfig.newBuilder()
            .setScope(scope)
            .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(true).build())
            .setIssuesSummaryFeatureConfig(
                IssuesSummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .setThreatActivitySummaryFeatureConfig(
                ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(true).build())
            .build();
    genAiConfigStore.upsertObject(requestContext, existingConfig);

    IssuesSummaryFeatureConfig issuesFeatureConfig =
        IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build();
    ThreatActivitySummaryFeatureConfig threatFeatureConfig =
        ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build();
    FeatureLevelConfigUpdate featureLevelUpdate =
        FeatureLevelConfigUpdate.newBuilder()
            .setIssuesSummaryFeatureConfigUpdate(issuesFeatureConfig)
            .setThreatActivitySummaryFeatureConfigUpdate(threatFeatureConfig)
            .build();

    UpdateGenAiConfigRequest request =
        UpdateGenAiConfigRequest.newBuilder()
            .setScope(scope)
            .setFeatureLevelConfigUpdate(featureLevelUpdate)
            .build();

    when(defaultGenAiConfigProvider.get()).thenReturn(createDefaultDisabledConfig());

    GenAiConfig result = manager.updateGenAiConfig(requestContext, scope, request);

    assertNotNull(result);
    assertTrue(result.hasGlobalConfig());
    assertTrue(result.getGlobalConfig().getEnabled());
    assertTrue(result.hasIssuesSummaryFeatureConfig());
    assertFalse(result.getIssuesSummaryFeatureConfig().getEnabled());
    assertTrue(result.hasThreatActivitySummaryFeatureConfig());
    assertFalse(result.getThreatActivitySummaryFeatureConfig().getEnabled());
    assertEquals(scope, result.getScope());
  }

  private GenAiConfig createGenAiConfig(boolean enabled) {
    return GenAiConfig.newBuilder()
        .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(enabled).build())
        .setIssuesSummaryFeatureConfig(
            IssuesSummaryFeatureConfig.newBuilder().setEnabled(enabled).build())
        .setThreatActivitySummaryFeatureConfig(
            ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(enabled).build())
        .setChatBotFeatureConfig(ChatbotFeatureConfig.newBuilder().setEnabled(enabled).build())
        .build();
  }

  private GenAiConfig createDefaultDisabledConfig() {
    return GenAiConfig.newBuilder()
        .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(false).build())
        .setIssuesSummaryFeatureConfig(
            IssuesSummaryFeatureConfig.newBuilder().setEnabled(false).build())
        .setThreatActivitySummaryFeatureConfig(
            ThreatActivitySummaryFeatureConfig.newBuilder().setEnabled(false).build())
        .build();
  }

  private GenAiConfig createConfigWithGlobalOnly(boolean globalEnabled) {
    return GenAiConfig.newBuilder()
        .setGlobalConfig(GlobalConfig.newBuilder().setEnabled(globalEnabled).build())
        .build();
  }
}
