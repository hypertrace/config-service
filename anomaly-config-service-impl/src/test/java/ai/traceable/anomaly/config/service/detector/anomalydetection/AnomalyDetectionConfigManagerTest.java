package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.common.AnomalySubRuleConfigUtils;
import ai.traceable.anomaly.config.service.detector.DetectorConfigServiceConfig;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.AnomalyDetectionConfigHandler;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.AnomalyDetectionConfigUtils;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.GlobalTestingModeResolver;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.ModsecConfigHandler;
import ai.traceable.anomaly.config.service.global.ruleinfo.RuleInfoManager;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistry;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistry;
import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.*;
import ai.traceable.anomaly.config.service.v1.detector.*;
import ai.traceable.anomaly.config.service.v1.detector.CredentialStuffingAnomalyDetectionConfig.ConfigCase;
import ai.traceable.anomaly.config.service.v1.global.ApiDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.GlobalApiConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AnomalyDetectionConfigManagerTest {
  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;

  private final ConfigConverter configConverter = new ConfigConverter();
  private final ApiDefinitionRegistry apiDefinitionRegistry =
      new ApiDefinitionRegistryImpl(configConverter);
  private final SessionRulesRegistry sessionRulesRegistry =
      new SessionRulesRegistryImpl(configConverter);
  private final VolumetricRulesRegistry volumetricRulesRegistry =
      new VolumetricRulesRegistryImpl(configConverter);
  private final CredentialStuffingRulesRegistry credentialStuffingRulesRegistry =
      new CredentialStuffingRulesRegistryImpl(configConverter);
  private final AccountTakeoverRulesRegistry accountTakeoverRulesRegistry =
      new AccountTakeoverRulesRegistryImpl(configConverter);
  private final GenAiRulesRegistry genAiRulesRegistry = new GenAiRulesRegistryImpl(configConverter);
  private final AnomalyDetectionConfigHandler detectionConfigConverter =
      new AnomalyDetectionConfigHandler(
          apiDefinitionRegistry,
          sessionRulesRegistry,
          volumetricRulesRegistry,
          credentialStuffingRulesRegistry,
          accountTakeoverRulesRegistry,
          genAiRulesRegistry);
  private WafConfigResolver wafConfigResolver;
  private AnomalyDetectionConfigManager configManager;
  private final AnomalyEnvironmentScope environmentScope =
      AnomalyEnvironmentScope.newBuilder().setEnvironmentId("environment").build();
  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();

  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope environmentConfigScope =
      AnomalyConfigScope.newBuilder().setEnvironmentScope(environmentScope).build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();

  private static final String DETECTOR_CONFIG_DIRECTORY = "detector/";
  private static final String SCOPED_DETECTION_CONFIGS_FILE_PATH =
      DETECTOR_CONFIG_DIRECTORY + "scoped-detection-configs.conf";
  private static final String RESOLVED_DETECTION_CONFIGS_FILE_PATH =
      DETECTOR_CONFIG_DIRECTORY + "resolved-detection-configs.conf";

  private static final String GLOBAL_RESOLVED_DETECTION_CONFIGS_FILE_PATH =
      DETECTOR_CONFIG_DIRECTORY + "global-resolved-detection-configs.conf";

  private static final String CUSTOMER_SCOPE_CONFIG = "customerScopeConfig";
  private static final String ENVIRONMENT_SCOPE_CONFIG = "environmentScopeConfig";
  private static final String SERVICE_SCOPE_CONFIG = "serviceScopeConfig";
  private static final String API_SCOPE_CONFIG = "apiScopeConfig";

  private static final Config scopedDetectionConfigs =
      ConfigFactory.parseResources(SCOPED_DETECTION_CONFIGS_FILE_PATH);
  private static final Config resolvedDetectionConfigs =
      ConfigFactory.parseResources(RESOLVED_DETECTION_CONFIGS_FILE_PATH);
  private static final Config globalResolvedDetectionConfigs =
      ConfigFactory.parseResources(GLOBAL_RESOLVED_DETECTION_CONFIGS_FILE_PATH);
  private AnomalyConfigScopeUtils anomalyConfigScopeUtils;

  private GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;
  private RuleInfoManager ruleInfoManager;
  private GlobalTestingModeResolver globalTestingModeResolver;

  @BeforeEach
  public void setup() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    Channel channelForMockServer = InProcessChannelBuilder.forName(serverName).build();
    mockServer = InProcessServerBuilder.forName(serverName).build().start();
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    DetectorConfigServiceConfig detectorConfigServiceConfig =
        mock(DetectorConfigServiceConfig.class);
    anomalyConfigScopeUtils = new AnomalyConfigScopeUtils();
    globalTestingModeResolver = mock(GlobalTestingModeResolver.class);
    when(detectorConfigServiceConfig.getDefaultWafDetectionConfigs()).thenReturn(List.of());
    when(detectorConfigServiceConfig.getDefaultGenAiDetectionConfigs()).thenReturn(List.of());
    when(detectorConfigServiceConfig.getDefaultApiProtectionDetectionConfigs())
        .thenReturn(List.of());
    when(globalTestingModeResolver.resolveGlobalTestingModeAndUpdateDetectionConfig(
            any(), any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    globalAnomalyConfigStatusManager = mock(GlobalAnomalyConfigStatusManager.class);
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(any(), any()))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_DEFAULT_ENABLED))
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            any(), eq(environmentConfigScope)))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_ENVIRONMENT))
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());
    ruleInfoManager = mock(RuleInfoManager.class);
    wafConfigResolver =
        new WafConfigResolver(
            ruleInfoManager, detectorConfigServiceConfig, new ModsecConfigHandler());

    this.configManager =
        spy(
            new AnomalyDetectionConfigManagerImpl(
                configServiceBlockingStub,
                detectionConfigConverter,
                anomalyConfigScopeUtils,
                detectorConfigServiceConfig,
                mock(ConfigChangeEventGenerator.class),
                globalAnomalyConfigStatusManager,
                wafConfigResolver,
                globalTestingModeResolver));
  }

  @AfterEach
  public void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdown();
  }

  @Test
  void testGetDetectionConfigs() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig;

    GetAnomalyDetectionConfigsFilter filter = GetAnomalyDetectionConfigsFilter.getDefaultInstance();

    assertThrows(
        RuntimeException.class,
        () ->
            configManager.getScopedAnomalyDetectionConfig(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                GetAnomalyDetectionConfigsFilter.getDefaultInstance()));

    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(requestContext, customerConfigScope, filter);
    assertEquals(customerConfigScope, scopedAnomalyDetectionConfig.getConfigScope());
    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);
    ScopedAnomalyDetectionConfig customerScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));

    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(requestContext, customerConfigScope, filter);
    assertEquals(customerScopeResolvedConfig, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);
    ScopedAnomalyDetectionConfig environmentScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(
            resolvedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, environmentConfigScope))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_ENVIRONMENT))
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());
    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(
            requestContext, environmentConfigScope, filter);
    assertEquals(environmentScopeResolvedConfig, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);
    ScopedAnomalyDetectionConfig serviceScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));

    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(requestContext, serviceConfigScope, filter);
    assertEquals(serviceScopeResolvedConfig, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(API_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);
    ScopedAnomalyDetectionConfig apiScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(API_SCOPE_CONFIG));

    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(requestContext, apiConfigScope, filter);
    assertEquals(apiScopeResolvedConfig, scopedAnomalyDetectionConfig);

    filter =
        GetAnomalyDetectionConfigsFilter.newBuilder()
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_STATE_BASED)
            .build();
    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(requestContext, customerConfigScope, filter);
    assertEquals(0, scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsCount());

    filter =
        GetAnomalyDetectionConfigsFilter.newBuilder()
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_DEFINITION)
            .build();
    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(requestContext, customerConfigScope, filter);
    assertEquals(1, scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsCount());

    filter =
        GetAnomalyDetectionConfigsFilter.newBuilder()
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY)
            .build();
    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(requestContext, apiConfigScope, filter);
    assertEquals(4, scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsCount());
  }

  @Test
  void testGetGlobalResolvedScopedAnomalyDetection() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig;
    ScopedAnomalyConfigStatus scopedAnomalyConfigStatus =
        ScopedAnomalyConfigStatus.newBuilder()
            .setConfigStatus(
                AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(true).build())
            .setGlobalModsecConfig(GlobalModsecConfig.newBuilder().setDisabled(true).build())
            .setGlobalApiConfig(GlobalApiConfig.newBuilder().setDisabled(true).build())
            .build();
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            any(RequestContext.class), any(AnomalyConfigScope.class)))
        .thenReturn(scopedAnomalyConfigStatus);
    assertThrows(
        RuntimeException.class,
        () ->
            configManager.getGlobalResolvedScopedAnomalyDetectionConfig(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                GetAnomalyDetectionConfigsFilter.getDefaultInstance()));
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, environmentConfigScope))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDisabled(true)
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_ENVIRONMENT))
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDisabled(true)
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(API_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig customerScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(
            globalResolvedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig environmentScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(
            globalResolvedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig serviceScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(
            globalResolvedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig apiScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(globalResolvedDetectionConfigs.getConfig(API_SCOPE_CONFIG));

    scopedAnomalyDetectionConfig =
        configManager.getGlobalResolvedScopedAnomalyDetectionConfig(
            requestContext,
            customerConfigScope,
            GetAnomalyDetectionConfigsFilter.getDefaultInstance());
    assertEquals(customerScopeResolvedConfig, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        configManager.getGlobalResolvedScopedAnomalyDetectionConfig(
            requestContext,
            environmentConfigScope,
            GetAnomalyDetectionConfigsFilter.getDefaultInstance());
    assertEquals(environmentScopeResolvedConfig, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        configManager.getGlobalResolvedScopedAnomalyDetectionConfig(
            requestContext,
            serviceConfigScope,
            GetAnomalyDetectionConfigsFilter.getDefaultInstance());
    assertEquals(serviceScopeResolvedConfig, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        configManager.getGlobalResolvedScopedAnomalyDetectionConfig(
            requestContext, apiConfigScope, GetAnomalyDetectionConfigsFilter.getDefaultInstance());
    assertEquals(apiScopeResolvedConfig, scopedAnomalyDetectionConfig);
  }

  @Test
  void testGetAllGlobalResolvedScopedAnomalyDetection() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    List<ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigs;
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig;
    GetAnomalyDetectionConfigsFilter filter = GetAnomalyDetectionConfigsFilter.getDefaultInstance();
    when(globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            any(RequestContext.class), any()))
        .thenReturn(createScopedAnomalyConfigStatusList());

    scopedAnomalyDetectionConfigs =
        configManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(requestContext, filter);
    assertEquals(1, scopedAnomalyDetectionConfigs.size());
    assertEquals(customerConfigScope, scopedAnomalyDetectionConfigs.get(0).getConfigScope());

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(API_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfigs =
        configManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(requestContext, filter);
    List<AnomalyConfigScope> configScopes =
        scopedAnomalyDetectionConfigs.stream()
            .map(ScopedAnomalyDetectionConfig::getConfigScope)
            .collect(Collectors.toList());

    assertEquals(4, scopedAnomalyDetectionConfigs.size());
    assertTrue(configScopes.contains(customerConfigScope));

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig customerScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(
            globalResolvedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig environmentScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(
            globalResolvedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig serviceScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(
            globalResolvedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig apiScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(globalResolvedDetectionConfigs.getConfig(API_SCOPE_CONFIG));

    scopedAnomalyDetectionConfigs =
        configManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(requestContext, filter);
    assertEquals(4, scopedAnomalyDetectionConfigs.size());

    assertEquals(
        customerScopeResolvedConfig, getConfig(customerConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(
        environmentScopeResolvedConfig,
        getConfig(environmentConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(
        serviceScopeResolvedConfig, getConfig(serviceConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(apiScopeResolvedConfig, getConfig(apiConfigScope, scopedAnomalyDetectionConfigs));
  }

  @Test
  void testGetAllDetectionConfigs() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig;
    List<ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigs;

    GetAnomalyDetectionConfigsFilter filter = GetAnomalyDetectionConfigsFilter.getDefaultInstance();
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, environmentConfigScope))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_ENVIRONMENT))
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());

    assertThrows(
        RuntimeException.class,
        () ->
            configManager.getScopedAnomalyDetectionConfig(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                GetAnomalyDetectionConfigsFilter.getDefaultInstance()));

    scopedAnomalyDetectionConfigs =
        configManager.getAllScopedAnomalyDetectionConfig(requestContext, filter);
    assertEquals(1, scopedAnomalyDetectionConfigs.size());
    assertEquals(customerConfigScope, scopedAnomalyDetectionConfigs.get(0).getConfigScope());

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(API_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfigs =
        configManager.getAllScopedAnomalyDetectionConfig(requestContext, filter);
    List<AnomalyConfigScope> configScopes =
        scopedAnomalyDetectionConfigs.stream()
            .map(ScopedAnomalyDetectionConfig::getConfigScope)
            .collect(Collectors.toList());

    assertEquals(4, scopedAnomalyDetectionConfigs.size());
    assertTrue(configScopes.contains(customerConfigScope));

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig customerScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig environmentScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(
            resolvedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig serviceScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig apiScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(API_SCOPE_CONFIG));

    scopedAnomalyDetectionConfigs =
        configManager.getAllScopedAnomalyDetectionConfig(requestContext, filter);
    assertEquals(4, scopedAnomalyDetectionConfigs.size());

    assertEquals(
        customerScopeResolvedConfig, getConfig(customerConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(
        environmentScopeResolvedConfig,
        getConfig(environmentConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(
        serviceScopeResolvedConfig, getConfig(serviceConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(apiScopeResolvedConfig, getConfig(apiConfigScope, scopedAnomalyDetectionConfigs));
  }

  @Test
  void testDefaultConfig() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    DetectorConfigServiceConfig config = getDefaultConfig();
    wafConfigResolver = new WafConfigResolver(ruleInfoManager, config, new ModsecConfigHandler());
    configManager =
        new AnomalyDetectionConfigManagerImpl(
            configServiceBlockingStub,
            detectionConfigConverter,
            anomalyConfigScopeUtils,
            config,
            mock(ConfigChangeEventGenerator.class),
            globalAnomalyConfigStatusManager,
            wafConfigResolver,
            globalTestingModeResolver);

    List<AnomalyDetectionConfig> defaultDetectionConfigs = new ArrayList<>();
    defaultDetectionConfigs.addAll(config.getDefaultWafDetectionConfigs());
    defaultDetectionConfigs.addAll(config.getDefaultApiProtectionDetectionConfigs());
    defaultDetectionConfigs.addAll(config.getDefaultGenAiDetectionConfigs());

    GetAnomalyDetectionConfigsFilter filter =
        GetAnomalyDetectionConfigsFilter.newBuilder()
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_STATE_BASED)
            .build();
    assertEquals(
        0,
        configManager
            .getScopedAnomalyDetectionConfig(requestContext, customerConfigScope, filter)
            .getAnomalyDetectionConfigsCount());
    assertEquals(
        0,
        configManager
            .getGlobalResolvedScopedAnomalyDetectionConfig(
                requestContext, customerConfigScope, filter)
            .getAnomalyDetectionConfigsCount());
    verifyEmptyDetectionConfigs(
        configManager.getAllScopedAnomalyDetectionConfig(requestContext, filter));
    verifyEmptyDetectionConfigs(
        configManager.getAllGlobalResolvedScopedAnomalyDetectionConfigs(requestContext, filter));

    List<AnomalyDetectionConfig> detectionConfigs =
        configManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                customerConfigScope,
                GetAnomalyDetectionConfigsFilter.getDefaultInstance())
            .getAnomalyDetectionConfigsList();
    assertEquals(defaultDetectionConfigs, detectionConfigs);

    detectionConfigs =
        configManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                environmentConfigScope,
                GetAnomalyDetectionConfigsFilter.getDefaultInstance())
            .getAnomalyDetectionConfigsList();
    assertEquals(defaultDetectionConfigs, detectionConfigs);

    detectionConfigs =
        configManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                serviceConfigScope,
                GetAnomalyDetectionConfigsFilter.getDefaultInstance())
            .getAnomalyDetectionConfigsList();
    assertEquals(defaultDetectionConfigs, detectionConfigs);

    detectionConfigs =
        configManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                apiConfigScope,
                GetAnomalyDetectionConfigsFilter.getDefaultInstance())
            .getAnomalyDetectionConfigsList();
    assertEquals(defaultDetectionConfigs, detectionConfigs);
  }

  @Test
  void testDefaultStandardModsecConfig() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    DetectorConfigServiceConfig config = getDefaultConfig();
    configManager =
        new AnomalyDetectionConfigManagerImpl(
            configServiceBlockingStub,
            detectionConfigConverter,
            anomalyConfigScopeUtils,
            config,
            mock(ConfigChangeEventGenerator.class),
            globalAnomalyConfigStatusManager,
            new WafConfigResolver(ruleInfoManager, config, new ModsecConfigHandler()),
            globalTestingModeResolver);
    List<AnomalyDetectionConfig> defaultDetectionConfigs = new ArrayList<>();
    defaultDetectionConfigs.add(
        config.getDefaultWafDetectionConfigs().get(1).toBuilder()
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .build());
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule1")
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(true)
                                            .build())
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE))
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setBlockingEnabled(false)
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(false)
                                            .build())
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR))
                            .build())
                    .build())
            .build());
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule2")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setBlockingEnabled(false)
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(false)
                                            .build())
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR))
                            .build())
                    .build())
            .build());
    defaultDetectionConfigs.add(
        config.getDefaultWafDetectionConfigs().get(0).toBuilder()
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .build());
    defaultDetectionConfigs.addAll(config.getDefaultApiProtectionDetectionConfigs());
    defaultDetectionConfigs.addAll(config.getDefaultGenAiDetectionConfigs());
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(any(), any()))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD))
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());
    when(ruleInfoManager.getModsecAnomalyRuleInfo(any(), any(), any(), anyBoolean()))
        .thenReturn(
            List.of(
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule1")
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule1")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
                            .build())
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule2")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                            .build())
                    .build(),
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule2")
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule2")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                            .build())
                    .build()));
    List<AnomalyDetectionConfig> detectionConfigs =
        configManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                environmentConfigScope,
                GetAnomalyDetectionConfigsFilter.getDefaultInstance())
            .getAnomalyDetectionConfigsList();
    assertEquals(defaultDetectionConfigs, detectionConfigs);
  }

  @Test
  void testPopulateNewFields() {
    AnomalySubRuleConfig oldStyleConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("old-style-config")
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build())
            .build();
    AnomalySubRuleConfig blockingEnabledOnlyConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("blocking-only-config")
            .setBlockingEnabled(true)
            .build();

    ScopedAnomalyDetectionConfig inputConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("test-rule")
                                    .addSubRuleConfigs(oldStyleConfig)
                                    .addSubRuleConfigs(blockingEnabledOnlyConfig)
                                    .build())
                            .build()))
            .build();
    ScopedAnomalyDetectionConfig processedConfig =
        AnomalyDetectionConfigUtils.populateNewFields(inputConfig);
    AnomalyDetectionConfig detectionConfig = processedConfig.getAnomalyDetectionConfigs(0);
    ModsecurityAnomalyRuleConfig ruleConfig =
        detectionConfig.getModsecurityAnomalyDetectionConfig().getModsecAnomalyRule();
    AnomalySubRuleConfig processedOldConfig = ruleConfig.getSubRuleConfigs(0);
    assertEquals("old-style-config", processedOldConfig.getSubRuleId());
    assertTrue(processedOldConfig.hasConfigStatus());
    assertTrue(processedOldConfig.getConfigStatus().getDisabled());
    assertTrue(processedOldConfig.getConfigStatus().hasInternal());
    assertTrue(processedOldConfig.getConfigStatus().getInternal());
    assertTrue(processedOldConfig.hasInternal());
    assertTrue(processedOldConfig.getInternal());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, processedOldConfig.getAnomalyRuleAction());

    // Verify blocking config
    AnomalySubRuleConfig processedBlockingConfig = ruleConfig.getSubRuleConfigs(1);
    assertEquals("blocking-only-config", processedBlockingConfig.getSubRuleId());
    assertTrue(processedBlockingConfig.hasBlockingEnabled());
    assertTrue(processedBlockingConfig.getBlockingEnabled());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK,
        processedBlockingConfig.getAnomalyRuleAction());

    inputConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setGenAiAnomalyDetectionConfig(
                        GenAiAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("genAi-rule")
                            .setSubRuleConfigs(
                                AnomalySubRuleConfigMap.newBuilder()
                                    .putAllSubRuleConfigs(
                                        Map.of(
                                            "genAi_subrule1",
                                            AnomalySubRuleConfig.newBuilder()
                                                .setSubRuleId("genAi_subrule1")
                                                .setConfigStatus(
                                                    AnomalyConfigStatusChange.newBuilder()
                                                        .setDisabled(true)
                                                        .build())
                                                .setAnomalyRuleAction(
                                                    AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE)
                                                .build(),
                                            "genAi_subrule2",
                                            AnomalySubRuleConfig.newBuilder()
                                                .setSubRuleId("genAi_subrule2")
                                                .setBlockingEnabled(true)
                                                .build()))))
                    .build())
            .build();

    processedConfig = AnomalyDetectionConfigUtils.populateNewFields(inputConfig);
    assertEquals(1, processedConfig.getAnomalyDetectionConfigsCount());
  }

  @Test
  void testBackwardCompatibilityInGetScopedAnomalyDetectionConfig() {
    AnomalySubRuleConfig oldStyleConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("old-style-config")
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(false).build())
            .build();
    AnomalySubRuleConfig processedOldConfig =
        AnomalySubRuleConfigUtils.populateNewFields(oldStyleConfig);
    assertTrue(processedOldConfig.hasConfigStatus());
    assertTrue(processedOldConfig.getConfigStatus().getDisabled());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, processedOldConfig.getAnomalyRuleAction());
    AnomalySubRuleConfig blockingEnabledConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("blocking-only-config")
            .setBlockingEnabled(true)
            .build();
    AnomalySubRuleConfig processedBlockingConfig =
        AnomalySubRuleConfigUtils.populateNewFields(blockingEnabledConfig);
    assertTrue(processedBlockingConfig.hasBlockingEnabled());
    assertTrue(processedBlockingConfig.getBlockingEnabled());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK,
        processedBlockingConfig.getAnomalyRuleAction());
  }

  @Test
  void testAnomalySubRuleConfigBackwardCompatibilityUtility() {
    AnomalySubRuleConfig oldStyleConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("old-style-config")
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build())
            .build();

    AnomalySubRuleConfig updatedConfig =
        AnomalySubRuleConfigUtils.populateNewFields(oldStyleConfig);
    assertTrue(updatedConfig.hasConfigStatus());
    assertTrue(updatedConfig.getConfigStatus().getDisabled());
    assertTrue(updatedConfig.getConfigStatus().hasInternal());
    assertTrue(updatedConfig.getConfigStatus().getInternal());
    assertTrue(updatedConfig.hasInternal());
    assertTrue(updatedConfig.getInternal());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, updatedConfig.getAnomalyRuleAction());

    AnomalySubRuleConfig blockingEnabledOnlyConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("blocking-only-config")
            .setBlockingEnabled(true)
            .build();

    updatedConfig = AnomalySubRuleConfigUtils.populateNewFields(blockingEnabledOnlyConfig);
    assertTrue(updatedConfig.hasBlockingEnabled());
    assertTrue(updatedConfig.getBlockingEnabled());
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK, updatedConfig.getAnomalyRuleAction());

    // Test when both disabled and blocking are not set - should be MONITOR
    AnomalySubRuleConfig neitherDisabledNorBlockingConfig =
        AnomalySubRuleConfig.newBuilder().setSubRuleId("monitor-config").build();

    updatedConfig = AnomalySubRuleConfigUtils.populateNewFields(neitherDisabledNorBlockingConfig);
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, updatedConfig.getAnomalyRuleAction());
    AnomalySubRuleConfig preferredConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .build();
    AnomalySubRuleConfig fallbackConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("test-sub-rule")
            .setBlockingEnabled(true)
            .build();

    AnomalySubRuleConfig mergedConfig =
        AnomalySubRuleConfigUtils.mergeAndPopulateNewFields(fallbackConfig, preferredConfig);
    assertTrue(mergedConfig.hasConfigStatus());
    assertTrue(mergedConfig.getConfigStatus().getDisabled());
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, mergedConfig.getAnomalyRuleAction());
  }

  @Test
  void testDefaultRecommmendedModsecConfig() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    DetectorConfigServiceConfig config = getDefaultConfig();
    configManager =
        new AnomalyDetectionConfigManagerImpl(
            configServiceBlockingStub,
            detectionConfigConverter,
            anomalyConfigScopeUtils,
            config,
            mock(ConfigChangeEventGenerator.class),
            globalAnomalyConfigStatusManager,
            wafConfigResolver,
            globalTestingModeResolver);
    List<AnomalyDetectionConfig> defaultDetectionConfigs = new ArrayList<>();
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule1")
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(true)
                                            .build())
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE))
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setBlockingEnabled(true)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
                            .build())
                    .build())
            .build());
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule2")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setBlockingEnabled(true)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
                            .build())
                    .build())
            .build());
    defaultDetectionConfigs.addAll(config.getDefaultApiProtectionDetectionConfigs());
    defaultDetectionConfigs.addAll(config.getDefaultGenAiDetectionConfigs());
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(any(), any()))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_RECOMMENDED))
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());
    when(ruleInfoManager.getModsecAnomalyRuleInfo(any(), any(), any(), anyBoolean()))
        .thenReturn(
            List.of(
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule1")
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule1")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
                            .build())
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule2")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                            .build())
                    .build(),
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule2")
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule2")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                            .build())
                    .build()));
    List<AnomalyDetectionConfig> detectionConfigs =
        configManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                environmentConfigScope,
                GetAnomalyDetectionConfigsFilter.getDefaultInstance())
            .getAnomalyDetectionConfigsList();
    assertEquals(defaultDetectionConfigs, detectionConfigs);
  }

  @Test
  void testDefaultStrictModsecConfig() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    DetectorConfigServiceConfig config = getDefaultConfig();
    configManager =
        new AnomalyDetectionConfigManagerImpl(
            configServiceBlockingStub,
            detectionConfigConverter,
            anomalyConfigScopeUtils,
            config,
            mock(ConfigChangeEventGenerator.class),
            globalAnomalyConfigStatusManager,
            wafConfigResolver,
            globalTestingModeResolver);
    List<AnomalyDetectionConfig> defaultDetectionConfigs = new ArrayList<>();
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule1")
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(false)
                                            .build())
                                    .setBlockingEnabled(false)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR))
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setBlockingEnabled(true)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
                            .build())
                    .build())
            .build());
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule2")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setBlockingEnabled(true)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
                            .build())
                    .build())
            .build());

    defaultDetectionConfigs.addAll(config.getDefaultApiProtectionDetectionConfigs());
    defaultDetectionConfigs.addAll(config.getDefaultGenAiDetectionConfigs());
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(any(), any()))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STRICT))
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());
    when(ruleInfoManager.getModsecAnomalyRuleInfo(any(), any(), any(), anyBoolean()))
        .thenReturn(
            List.of(
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule1")
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule1")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
                            .build())
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule2")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                            .build())
                    .build(),
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule2")
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule2")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                            .build())
                    .build()));
    List<AnomalyDetectionConfig> detectionConfigs =
        configManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                environmentConfigScope,
                GetAnomalyDetectionConfigsFilter.getDefaultInstance())
            .getAnomalyDetectionConfigsList();
    assertEquals(defaultDetectionConfigs, detectionConfigs);
  }

  @Test
  void testDefaultPerEnv() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    DetectorConfigServiceConfig config = getDefaultConfig();
    configManager =
        new AnomalyDetectionConfigManagerImpl(
            configServiceBlockingStub,
            detectionConfigConverter,
            anomalyConfigScopeUtils,
            config,
            mock(ConfigChangeEventGenerator.class),
            globalAnomalyConfigStatusManager,
            wafConfigResolver,
            globalTestingModeResolver);
    List<AnomalyDetectionConfig> defaultDetectionConfigs = new ArrayList<>();
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule1")
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(true)
                                            .build())
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE))
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(false)
                                            .build())
                                    .setBlockingEnabled(false)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR))
                            .build())
                    .build())
            .build());
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule2")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setBlockingEnabled(false)
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(false)
                                            .build())
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR))
                            .build())
                    .build())
            .build());
    defaultDetectionConfigs.addAll(config.getDefaultApiProtectionDetectionConfigs());
    defaultDetectionConfigs.addAll(config.getDefaultGenAiDetectionConfigs());
    when(ruleInfoManager.getModsecAnomalyRuleInfo(any(), any(), any(), anyBoolean()))
        .thenReturn(
            List.of(
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule1")
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule1")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
                            .build())
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule2")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                            .build())
                    .build(),
                AnomalyRuleInfo.newBuilder()
                    .setRuleId("rule2")
                    .addSubRuleInfos(
                        AnomalySubRuleInfo.newBuilder()
                            .setRuleId("subrule2")
                            .addSubRuleTypes(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                            .build())
                    .build()));
    List<AnomalyDetectionConfig> detectionConfigs;

    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, environmentConfigScope))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD)
                        .build())
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());
    doReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STRICT)
                        .build())
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build())
        .when(globalAnomalyConfigStatusManager)
        .getScopedAnomalyConfigStatus(requestContext, customerConfigScope);

    detectionConfigs =
        configManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                environmentConfigScope,
                GetAnomalyDetectionConfigsFilter.getDefaultInstance())
            .getAnomalyDetectionConfigsList();
    assertEquals(defaultDetectionConfigs, detectionConfigs);

    defaultDetectionConfigs.clear();
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule1")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule1")
                                    .setBlockingEnabled(false)
                                    .setConfigStatus(
                                        AnomalyConfigStatusChange.newBuilder()
                                            .setDisabled(false)
                                            .build())
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR))
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setBlockingEnabled(true)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
                            .build())
                    .build())
            .build());
    defaultDetectionConfigs.add(
        AnomalyDetectionConfig.newBuilder()
            .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setModsecurityAnomalyDetectionConfig(
                ModsecurityAnomalyDetectionConfig.newBuilder()
                    .setModsecAnomalyRule(
                        ModsecurityAnomalyRuleConfig.newBuilder()
                            .setAnomalyRuleId("rule2")
                            .addSubRuleConfigs(
                                AnomalySubRuleConfig.newBuilder()
                                    .setSubRuleId("subrule2")
                                    .setBlockingEnabled(true)
                                    .setAnomalyRuleAction(
                                        AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK))
                            .build())
                    .build())
            .build());
    defaultDetectionConfigs.addAll(config.getDefaultApiProtectionDetectionConfigs());
    defaultDetectionConfigs.addAll(config.getDefaultGenAiDetectionConfigs());
    when(globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, environmentConfigScope))
        .thenReturn(
            ScopedAnomalyConfigStatus.newBuilder()
                .setGlobalModsecConfig(
                    GlobalModsecConfig.newBuilder()
                        .setDefaultConfigsType(
                            ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_ENVIRONMENT))
                .setGlobalApiConfig(
                    GlobalApiConfig.newBuilder()
                        .setDefaultConfigsType(
                            ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED))
                .build());
    detectionConfigs =
        configManager
            .getScopedAnomalyDetectionConfig(
                requestContext,
                environmentConfigScope,
                GetAnomalyDetectionConfigsFilter.getDefaultInstance())
            .getAnomalyDetectionConfigsList();
    assertEquals(defaultDetectionConfigs, detectionConfigs);
  }

  @Test
  void testGetUnresolvedDetectionConfig() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig;

    assertThrows(
        RuntimeException.class,
        () ->
            configManager.getUnresolvedScopedAnomalyDetectionConfig(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                GetAnomalyDetectionConfigsFilter.getDefaultInstance()));

    ScopedAnomalyDetectionConfig customerScopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, customerScopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig serviceScopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, serviceScopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig environmentScopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(
            resolvedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, environmentScopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig apiScopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(API_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, apiScopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        requestContext.call(
            () ->
                configManager.getUnresolvedScopedAnomalyDetectionConfig(
                    requestContext,
                    customerConfigScope,
                    GetAnomalyDetectionConfigsFilter.getDefaultInstance()));
    assertEquals(customerScopedAnomalyDetectionConfig, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        requestContext.call(
            () ->
                configManager.getUnresolvedScopedAnomalyDetectionConfig(
                    requestContext,
                    environmentConfigScope,
                    GetAnomalyDetectionConfigsFilter.getDefaultInstance()));
    assertEquals(environmentScopedAnomalyDetectionConfig, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        requestContext.call(
            () ->
                configManager.getUnresolvedScopedAnomalyDetectionConfig(
                    requestContext,
                    serviceConfigScope,
                    GetAnomalyDetectionConfigsFilter.getDefaultInstance()));
    assertEquals(serviceScopedAnomalyDetectionConfig, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        requestContext.call(
            () ->
                configManager.getUnresolvedScopedAnomalyDetectionConfig(
                    requestContext,
                    apiConfigScope,
                    GetAnomalyDetectionConfigsFilter.getDefaultInstance()));
    assertEquals(apiScopedAnomalyDetectionConfig, scopedAnomalyDetectionConfig);
  }

  @Test
  void testGetAllUnresolvedDetectionConfigs() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    List<ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigs;

    GetAnomalyDetectionConfigsFilter filter = GetAnomalyDetectionConfigsFilter.getDefaultInstance();

    scopedAnomalyDetectionConfigs =
        configManager.getAllUnresolvedScopedAnomalyDetectionConfigs(requestContext, filter);
    assertEquals(2, scopedAnomalyDetectionConfigs.size());
    assertEquals(customerConfigScope, scopedAnomalyDetectionConfigs.get(0).getConfigScope());

    ScopedAnomalyDetectionConfig environmentScopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(
            resolvedDetectionConfigs.getConfig(ENVIRONMENT_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, environmentScopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig serviceScopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, serviceScopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig apiScopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(API_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, apiScopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfigs =
        configManager.getAllUnresolvedScopedAnomalyDetectionConfigs(requestContext, filter);
    List<AnomalyConfigScope> configScopes =
        scopedAnomalyDetectionConfigs.stream()
            .map(ScopedAnomalyDetectionConfig::getConfigScope)
            .collect(Collectors.toList());

    assertEquals(5, scopedAnomalyDetectionConfigs.size());
    assertTrue(configScopes.contains(customerConfigScope));

    ScopedAnomalyDetectionConfig customerScopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, customerScopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfigs =
        configManager.getAllUnresolvedScopedAnomalyDetectionConfigs(requestContext, filter);
    assertEquals(5, scopedAnomalyDetectionConfigs.size());

    assertEquals(
        customerScopedAnomalyDetectionConfig,
        getConfig(customerConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(
        environmentScopedAnomalyDetectionConfig,
        getConfig(environmentConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(
        serviceScopedAnomalyDetectionConfig,
        getConfig(serviceConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(
        apiScopedAnomalyDetectionConfig, getConfig(apiConfigScope, scopedAnomalyDetectionConfigs));
  }

  @Test
  void testDeleteDetectionConfig() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig;
    ScopedAnomalyDetectionConfig deletedScopedAnomalyDetectionConfig;

    assertThrows(
        RuntimeException.class,
        () ->
            configManager.deleteScopedAnomalyDetectionConfig(
                requestContext,
                ScopedAnomalyDetectionConfig.newBuilder()
                    .setConfigScope(
                        AnomalyConfigScope.newBuilder()
                            .setParamScope(AnomalyParamScope.getDefaultInstance())
                            .build())
                    .build(),
                DeleteAnomalyConfigOption.DELETE_ANOMALY_CONFIG_OPTION_WHOLE_DETECTION_CONFIG));

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    AnomalyDetectionConfig detectionConfig1 =
        getApiDefConfig(
            scopedAnomalyDetectionConfig,
            ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.INTEGER);
    AnomalyDetectionConfig detectionConfig2 =
        getSessionDefConfig(
            scopedAnomalyDetectionConfig,
            SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.OBJECT_BOLA);
    AnomalyDetectionConfig detectionConfig3 =
        getBlockingMetadataConfig(
            scopedAnomalyDetectionConfig,
            BlockingMetadataAnomalyDetectionConfig.ConfigCase.CUSTOM_IP);
    AnomalyDetectionConfig detectionConfig4 =
        getModsecRuleConfig(scopedAnomalyDetectionConfig, "rule1");
    AnomalyDetectionConfig detectionConfig5 =
        getModsecAllDetectionConfig(scopedAnomalyDetectionConfig);
    AnomalyDetectionConfig detectionConfig6 =
        getVolumetricConfig(
            scopedAnomalyDetectionConfig,
            VolumetricAnomalyDetectionConfig.ConfigCase.API_CALL_SPIKE);
    AnomalyDetectionConfig detectionConfig7 =
        getCredentialStuffingConfig(scopedAnomalyDetectionConfig, ConfigCase.CREDENTIAL_STUFFING);

    deletedScopedAnomalyDetectionConfig =
        requestContext.call(
            () ->
                configManager.deleteScopedAnomalyDetectionConfig(
                    requestContext,
                    ScopedAnomalyDetectionConfig.newBuilder()
                        .setConfigScope(customerConfigScope)
                        .addAnomalyDetectionConfigs(
                            AnomalyDetectionConfig.newBuilder()
                                .setApiDefinitionMetadataAnomalyDetectionConfig(
                                    ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                                        .setInteger(IntegerAnomalyConfig.getDefaultInstance())
                                        .build()))
                        .addAnomalyDetectionConfigs(
                            AnomalyDetectionConfig.newBuilder()
                                .setModsecurityAnomalyDetectionConfig(
                                    ModsecurityAnomalyDetectionConfig.newBuilder()
                                        .setModsecAnomalyRule(
                                            ModsecurityAnomalyRuleConfig.newBuilder()
                                                .setAnomalyRuleId("rule1")
                                                .build())
                                        .build()))
                        .addAnomalyDetectionConfigs(
                            AnomalyDetectionConfig.newBuilder()
                                .setModsecurityAnomalyDetectionConfig(
                                    ModsecurityAnomalyDetectionConfig.newBuilder()
                                        .setModsecAllDetection(
                                            ModsecurityAllDetectionConfig.getDefaultInstance())))
                        .addAnomalyDetectionConfigs(
                            AnomalyDetectionConfig.newBuilder()
                                .setSessionDefinitionMetadataAnomalyDetectionConfig(
                                    SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                                        .setObjectBola(
                                            ObjectBolaAnomalyConfig.getDefaultInstance())))
                        .addAnomalyDetectionConfigs(
                            AnomalyDetectionConfig.newBuilder()
                                .setBlockingMetadataAnomalyDetectionConfig(
                                    BlockingMetadataAnomalyDetectionConfig.newBuilder()
                                        .setCustomIp(CustomIpAnomalyConfig.getDefaultInstance())))
                        .addAnomalyDetectionConfigs(
                            AnomalyDetectionConfig.newBuilder()
                                .setVolumetricAnomalyDetectionConfig(
                                    VolumetricAnomalyDetectionConfig.newBuilder()
                                        .setApiCallSpike(
                                            ApiCallSpikeAnomalyConfig.getDefaultInstance())))
                        .addAnomalyDetectionConfigs(
                            AnomalyDetectionConfig.newBuilder()
                                .setCredentialAnomalyDetectionConfig(
                                    CredentialStuffingAnomalyDetectionConfig.newBuilder()
                                        .setCredentialStuffing(
                                            CredentialStuffingAnomalyConfig.getDefaultInstance())))
                        .build(),
                    DeleteAnomalyConfigOption.DELETE_ANOMALY_CONFIG_OPTION_WHOLE_DETECTION_CONFIG));

    ScopedAnomalyDetectionConfig expectedConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addAnomalyDetectionConfigs(detectionConfig1)
            .addAnomalyDetectionConfigs(detectionConfig2)
            .addAnomalyDetectionConfigs(detectionConfig3)
            .addAnomalyDetectionConfigs(detectionConfig4)
            .addAnomalyDetectionConfigs(detectionConfig5)
            .addAnomalyDetectionConfigs(detectionConfig6)
            .addAnomalyDetectionConfigs(detectionConfig7)
            .build();
    assertEquals(expectedConfig, deletedScopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(
            requestContext,
            customerConfigScope,
            GetAnomalyDetectionConfigsFilter.getDefaultInstance());

    assertEquals(0, scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsCount());
  }

  private void updateScopedAnomalyDetectionConfig(
      RequestContext requestContext, ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    requestContext.run(
        () ->
            configManager.updateScopedAnomalyDetectionConfig(
                requestContext, scopedAnomalyDetectionConfig));
  }

  private ScopedAnomalyDetectionConfig getScopedAnomalyDetectionConfig(Config config)
      throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig.Builder builder = ScopedAnomalyDetectionConfig.newBuilder();
    JsonFormat.parser()
        .ignoringUnknownFields()
        .merge(config.root().render(ConfigRenderOptions.concise()), builder);
    return builder.build();
  }

  private ScopedAnomalyDetectionConfig getConfig(
      AnomalyConfigScope configScope, List<ScopedAnomalyDetectionConfig> detectionConfigs) {

    for (ScopedAnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig.getConfigScope().equals(configScope)) {
        return detectionConfig;
      }
    }
    return null;
  }

  private DetectorConfigServiceConfig getDefaultConfig() {
    return new DetectorConfigServiceConfig(
        ConfigFactory.parseString(
            "modsecDetectionConfigs =\n"
                + "    [\n"
                + "      {\n"
                + "        configStatus = {\n"
                + "          disabled = false\n"
                + "          internal = false\n"
                + "        }\n"
                + "        modsecurityAnomalyDetectionConfig = {\n"
                + "           modsecAnomalyRule = {\n"
                + "            anomalyRuleId = \"crs_912\"\n"
                + "          }\n"
                + "        }\n"
                + "      },\n"
                + "      {\n"
                + "        configStatus = {\n"
                + "          disabled = true\n"
                + "          internal = false\n"
                + "        }\n"
                + "        modsecurityAnomalyDetectionConfig = {\n"
                + "           modsecAnomalyRule = {\n"
                + "            anomalyRuleId = \"crs_913\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    ]\n"
                + "apiDefinitionDetectionConfigs = [\n"
                + "    {\n"
                + "      configStatus = {\n"
                + "        disabled = false\n"
                + "        internal = false\n"
                + "      }\n"
                + "      apiDefinitionMetadataAnomalyDetectionConfig = {\n"
                + "        anomalyRuleId = \"missingParam\"\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      configStatus = {\n"
                + "        disabled = true\n"
                + "        internal = true\n"
                + "      }\n"
                + "      apiDefinitionMetadataAnomalyDetectionConfig = {\n"
                + "        anomalyRuleId = \"enum\"\n"
                + "      }\n"
                + "    }\n"
                + " ]\n"
                + "sessionDefinitionDetectionConfigs = [\n"
                + " {\n"
                + "   configStatus = {\n"
                + "        disabled = false\n"
                + "        internal = true\n"
                + "      }\n"
                + "      sessionDefinitionMetadataAnomalyDetectionConfig = {\n"
                + "        anomalyRuleId = \"bola\"\n"
                + "      }\n"
                + "    }\n"
                + "]\n"
                + "genAiDetectionConfigs = [\n"
                + "    {\n"
                + "      genAiAnomalyDetectionConfig = {\n"
                + "        anomalyRuleId = \"codeDetectedInPrompt\"\n"
                + "        codeDetectedInPrompt = {\n"
                + "          threatRuleConfigs = [\n"
                + "            {\n"
                + "              threatRuleId = \"crs_941\"\n"
                + "              subRuleIds = {\n"
                + "                values = [\"crs_9410170\", \"crs_9410160\", \"crs_941110\"]\n"
                + "              }\n"
                + "            }\n"
                + "            {\n"
                + "              threatRuleId = \"crs_942\"\n"
                + "              subRuleIds = {\n"
                + "                values = [\"crs_9420190\", \"crs_9420291\"]\n"
                + "              }\n"
                + "            }\n"
                + "          ]"
                + "        }\n"
                + "      subRuleConfigs = {\n"
                + "        subRuleConfigs = {\n"
                + "          crs_941 = {\n"
                + "            subRuleId = \"crs_941\"\n"
                + "            categoryConfig = {\n"
                + "              eventScoreCategory = ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM\n"
                + "            }\n"
                + "            anomalyRuleAction: ANOMALY_RULE_ACTION_MONITOR\n"
                + "          }\n"
                + "          crs_942 = {\n"
                + "            subRuleId = \"crs_942\"\n"
                + "            categoryConfig = {\n"
                + "              eventScoreCategory = ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM\n"
                + "            }\n"
                + "            anomalyRuleAction: ANOMALY_RULE_ACTION_MONITOR\n"
                + "          }\n"
                + "        }\n"
                + "      }"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "volumetricDetectionConfigs = [\n"
                + " {\n"
                + "   configStatus = {\n"
                + "        disabled = true\n"
                + "        internal = true\n"
                + "      }\n"
                + "      volumetricAnomalyDetectionConfig = {\n"
                + "        anomalyRuleId = \"volumetricApiCallSpike\"\n"
                + "      }\n"
                + "    }\n"
                + "]\n"
                + "credentialStuffingDetectionConfigs = [\n"
                + " {\n"
                + "   configStatus = {\n"
                + "        disabled = true\n"
                + "        internal = true\n"
                + "      }\n"
                + "      credentialAnomalyDetectionConfig = {\n"
                + "        anomalyRuleId = \"credentialStuffing\"\n"
                + "      }\n"
                + "    }\n"
                + "]\n"
                + "accountTakeoverDetectionConfigs = [\n"
                + " {\n"
                + "   configStatus = {\n"
                + "        disabled = true\n"
                + "        internal = true\n"
                + "      }\n"
                + "      accountTakeoverAnomalyDetectionConfig = {\n"
                + "        anomalyRuleId = \"ato\"\n"
                + "      }\n"
                + "    }\n"
                + "]\n"
                + "customRulesDetectionConfigs = [\n"
                + "    {\n"
                + "      categoryConfig = {\n"
                + "        eventScoreCategory = \"ANOMALY_EVENT_SCORE_CATEGORY_HIGH\"\n"
                + "      }\n"
                + "      customRulesAnomalyDetectionConfig = {\n"
                + "        maliciousSources = {\n"
                + "          emailDomain = {\n"
                + "            highEmailFraudScoreMinThreshold = 85\n"
                + "            criticalEmailFraudScoreMinThreshold = 95\n"
                + "            disabled = true\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      categoryConfig = {\n"
                + "        eventScoreCategory = \"ANOMALY_EVENT_SCORE_CATEGORY_HIGH\"\n"
                + "      }\n"
                + "      customRulesAnomalyDetectionConfig = {\n"
                + "        maliciousSources = {\n"
                + "          ipType = {\n"
                + "            abuseVelocityMinThreshold = \"ABUSE_VELOCITY_HIGH\"\n"
                + "            ipReputationScoreMinThreshold = 100\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]"),
        new ApiDefinitionRegistryImpl(new ConfigConverter()),
        new SessionRulesRegistryImpl(new ConfigConverter()),
        new VolumetricRulesRegistryImpl(new ConfigConverter()),
        new CredentialStuffingRulesRegistryImpl(new ConfigConverter()),
        new AccountTakeoverRulesRegistryImpl(new ConfigConverter()),
        new GenAiRulesRegistryImpl(new ConfigConverter()));
  }

  private AnomalyDetectionConfig getModsecRuleConfig(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig, String ruleId) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig
                    .getModsecurityAnomalyDetectionConfig()
                    .hasModsecAnomalyRule())
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig
                    .getModsecurityAnomalyDetectionConfig()
                    .getModsecAnomalyRule()
                    .getAnomalyRuleId()
                    .equals(ruleId))
        .findFirst()
        .orElseThrow();
  }

  private AnomalyDetectionConfig getModsecAllDetectionConfig(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig
                    .getModsecurityAnomalyDetectionConfig()
                    .hasModsecAllDetection())
        .findFirst()
        .orElseThrow();
  }

  private AnomalyDetectionConfig getApiDefConfig(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig.hasApiDefinitionMetadataAnomalyDetectionConfig())
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig
                    .getApiDefinitionMetadataAnomalyDetectionConfig()
                    .getConfigCase()
                    .equals(configCase))
        .findFirst()
        .orElseThrow();
  }

  private AnomalyDetectionConfig getBlockingMetadataConfig(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      BlockingMetadataAnomalyDetectionConfig.ConfigCase configCase) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig.hasBlockingMetadataAnomalyDetectionConfig())
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig
                    .getBlockingMetadataAnomalyDetectionConfig()
                    .getConfigCase()
                    .equals(configCase))
        .findFirst()
        .orElseThrow();
  }

  private AnomalyDetectionConfig getSessionDefConfig(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig.hasSessionDefinitionMetadataAnomalyDetectionConfig())
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig
                    .getSessionDefinitionMetadataAnomalyDetectionConfig()
                    .getConfigCase()
                    .equals(configCase))
        .findFirst()
        .orElseThrow();
  }

  private List<AnomalyConfigScope> getConfigScopes(
      List<ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigs) {
    return scopedAnomalyDetectionConfigs.stream()
        .map(ScopedAnomalyDetectionConfig::getConfigScope)
        .collect(Collectors.toList());
  }

  private AnomalyDetectionConfig getVolumetricConfig(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      VolumetricAnomalyDetectionConfig.ConfigCase configCase) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasVolumetricAnomalyDetectionConfig)
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig
                    .getVolumetricAnomalyDetectionConfig()
                    .getConfigCase()
                    .equals(configCase))
        .findFirst()
        .orElseThrow();
  }

  private AnomalyDetectionConfig getCredentialStuffingConfig(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      CredentialStuffingAnomalyDetectionConfig.ConfigCase configCase) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasCredentialAnomalyDetectionConfig)
        .filter(
            anomalyDetectionConfig ->
                anomalyDetectionConfig
                    .getCredentialAnomalyDetectionConfig()
                    .getConfigCase()
                    .equals(configCase))
        .findFirst()
        .orElseThrow();
  }

  public List<ScopedAnomalyConfigStatus> createScopedAnomalyConfigStatusList() {
    ScopedAnomalyConfigStatus scopedAnomalyConfigStatus1 =
        ScopedAnomalyConfigStatus.newBuilder()
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
            .setConfigScope(apiConfigScope)
            .setGlobalModsecConfig(GlobalModsecConfig.newBuilder().setDisabled(true).build())
            .setGlobalApiConfig(GlobalApiConfig.newBuilder().setDisabled(true).build())
            .build();
    ScopedAnomalyConfigStatus scopedAnomalyConfigStatus2 =
        ScopedAnomalyConfigStatus.newBuilder()
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
            .setGlobalModsecConfig(GlobalModsecConfig.newBuilder().setDisabled(true).build())
            .setGlobalApiConfig(GlobalApiConfig.newBuilder().setDisabled(true).build())
            .setConfigScope(serviceConfigScope)
            .build();
    ScopedAnomalyConfigStatus scopedAnomalyConfigStatus3 =
        ScopedAnomalyConfigStatus.newBuilder()
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
            .setGlobalModsecConfig(GlobalModsecConfig.newBuilder().setDisabled(true).build())
            .setGlobalApiConfig(GlobalApiConfig.newBuilder().setDisabled(true).build())
            .setConfigScope(environmentConfigScope)
            .build();
    ScopedAnomalyConfigStatus scopedAnomalyConfigStatus4 =
        ScopedAnomalyConfigStatus.newBuilder()
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
            .setGlobalModsecConfig(GlobalModsecConfig.newBuilder().setDisabled(true).build())
            .setGlobalApiConfig(GlobalApiConfig.newBuilder().setDisabled(true).build())
            .setConfigScope(customerConfigScope)
            .build();

    return Arrays.asList(
        scopedAnomalyConfigStatus1,
        scopedAnomalyConfigStatus2,
        scopedAnomalyConfigStatus3,
        scopedAnomalyConfigStatus4);
  }

  private void verifyEmptyDetectionConfigs(List<ScopedAnomalyDetectionConfig> detectionConfigs) {
    assertEquals(1, detectionConfigs.size());
    assertEquals(
        AnomalyConfigScope.ScopeCase.CUSTOMER_SCOPE,
        detectionConfigs.get(0).getConfigScope().getScopeCase());
    assertEquals(0, detectionConfigs.get(0).getAnomalyDetectionConfigsCount());
  }
}
