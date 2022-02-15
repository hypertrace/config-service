package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.detector.DetectorConfigServiceConfig;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
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
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AnomalyDetectionConfigManagerTest {
  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private static Channel channelForMockServer;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;

  private final ConfigConverter configConverter = new ConfigConverter();
  private final ApiDefinitionRegistry apiDefinitionRegistry =
      new ApiDefinitionRegistryImpl(configConverter);
  private final SessionRulesRegistry sessionRulesRegistry =
      new SessionRulesRegistryImpl(configConverter);
  private AnomalyDetectionConfigConverter detectionConfigConverter =
      new AnomalyDetectionConfigConverter(apiDefinitionRegistry, sessionRulesRegistry);
  private AnomalyDetectionConfigManager configManager;

  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();

  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();

  private static final String DETECTOR_CONFIG_DIRECTORY = "detector/";
  private static final String SCOPED_DETECTION_CONFIGS_FILE_PATH =
      DETECTOR_CONFIG_DIRECTORY + "scoped-detection-configs.conf";
  private static final String RESOLVED_DETECTION_CONFIGS_FILE_PATH =
      DETECTOR_CONFIG_DIRECTORY + "resolved-detection-configs.conf";

  private static final String CUSTOMER_SCOPE_CONFIG = "customerScopeConfig";
  private static final String SERVICE_SCOPE_CONFIG = "serviceScopeConfig";
  private static final String API_SCOPE_CONFIG = "apiScopeConfig";

  private static final Config scopedDetectionConfigs =
      ConfigFactory.parseResources(SCOPED_DETECTION_CONFIGS_FILE_PATH);
  private static final Config resolvedDetectionConfigs =
      ConfigFactory.parseResources(RESOLVED_DETECTION_CONFIGS_FILE_PATH);
  private DetectorConfigServiceConfig detectorConfigServiceConfig;

  @BeforeEach
  public void setup() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    channelForMockServer = InProcessChannelBuilder.forName(serverName).build();
    mockServer = InProcessServerBuilder.forName(serverName).build().start();
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    detectorConfigServiceConfig = mock(DetectorConfigServiceConfig.class);
    when(detectorConfigServiceConfig.getDefaultModsecDetectionConfigs()).thenReturn(List.of());
    when(detectorConfigServiceConfig.getDefaultApiDefinitionDetectionConfigs())
        .thenReturn(List.of());
    this.configManager =
        spy(
            new AnomalyDetectionConfigManagerImpl(
                configServiceBlockingStub, detectionConfigConverter, detectorConfigServiceConfig));
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
  void testGetAllDetectionConfigs() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig;
    List<ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigs;

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

    scopedAnomalyDetectionConfigs =
        configManager.getAllScopedAnomalyDetectionConfig(requestContext, filter);
    assertEquals(1, scopedAnomalyDetectionConfigs.size());
    assertEquals(customerConfigScope, scopedAnomalyDetectionConfigs.get(0).getConfigScope());

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

    assertEquals(3, scopedAnomalyDetectionConfigs.size());
    assertTrue(configScopes.contains(customerConfigScope));

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig customerScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig serviceScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig apiScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(API_SCOPE_CONFIG));

    scopedAnomalyDetectionConfigs =
        configManager.getAllScopedAnomalyDetectionConfig(requestContext, filter);
    assertEquals(3, scopedAnomalyDetectionConfigs.size());

    assertEquals(
        customerScopeResolvedConfig, getConfig(customerConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(
        serviceScopeResolvedConfig, getConfig(serviceConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(apiScopeResolvedConfig, getConfig(apiConfigScope, scopedAnomalyDetectionConfigs));
  }

  @Test
  void testDefaultConfig() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    DetectorConfigServiceConfig config = getDefaultConfig();
    configManager =
        new AnomalyDetectionConfigManagerImpl(
            configServiceBlockingStub, detectionConfigConverter, config);

    List<AnomalyDetectionConfig> defaultDetectionConfigs = new ArrayList<>();
    defaultDetectionConfigs.addAll(config.getDefaultModsecDetectionConfigs());
    defaultDetectionConfigs.addAll(config.getDefaultApiDefinitionDetectionConfigs());
    defaultDetectionConfigs.addAll(config.getDefaultSessionDefinitionDetectionConfigs());

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
                + "          anomalyRuleId = \"crs_912\"\n"
                + "        }\n"
                + "      },\n"
                + "      {\n"
                + "        configStatus = {\n"
                + "          disabled = true\n"
                + "          internal = false\n"
                + "        }\n"
                + "        modsecurityAnomalyDetectionConfig = {\n"
                + "          anomalyRuleId = \"crs_913\"\n"
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
                + "]"),
        new ApiDefinitionRegistryImpl(new ConfigConverter()),
        new SessionRulesRegistryImpl(new ConfigConverter()));
  }
}
