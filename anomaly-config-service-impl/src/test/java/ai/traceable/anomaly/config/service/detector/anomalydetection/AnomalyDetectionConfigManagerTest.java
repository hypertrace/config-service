package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

import ai.traceable.anomaly.config.service.detector.DetectorConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
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
import java.util.List;
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

  private AnomalyDetectionConfigConverter configConverter;
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

  @BeforeEach
  public void setup() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    channelForMockServer = InProcessChannelBuilder.forName(serverName).build();
    mockServer = InProcessServerBuilder.forName(serverName).build().start();
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    configConverter = new AnomalyDetectionConfigConverter();
    Config config = ConfigFactory.parseString("modsecDetectionConfigs = []");
    this.configManager =
        spy(
            new AnomalyDetectionConfigManagerImpl(
                configServiceBlockingStub,
                configConverter,
                new DetectorConfigServiceConfig(config)));
  }

  @AfterEach
  public void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdown();
  }

  @Test
  void testGetModsecConfigs() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig;

    GetAnomalyDetectionConfigsFilter filter =
        GetAnomalyDetectionConfigsFilter.newBuilder()
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_STATE_BASED)
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_DEFINITION)
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY)
            .build();

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
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_DEFINITION)
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_STATE_BASED)
            .build();
    scopedAnomalyDetectionConfig =
        configManager.getScopedAnomalyDetectionConfig(requestContext, apiConfigScope, filter);
    assertEquals(0, scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsCount());
  }

  @Test
  void testGetAllModsecConfigs() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig;

    GetAnomalyDetectionConfigsFilter filter =
        GetAnomalyDetectionConfigsFilter.newBuilder()
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_STATE_BASED)
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_API_DEFINITION)
            .addAnomalyDetectionConfigTypes(
                AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY)
            .build();

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
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    scopedAnomalyDetectionConfig =
        getScopedAnomalyDetectionConfig(scopedDetectionConfigs.getConfig(API_SCOPE_CONFIG));
    updateScopedAnomalyDetectionConfig(requestContext, scopedAnomalyDetectionConfig);

    ScopedAnomalyDetectionConfig customerScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig serviceScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    ScopedAnomalyDetectionConfig apiScopeResolvedConfig =
        getScopedAnomalyDetectionConfig(resolvedDetectionConfigs.getConfig(API_SCOPE_CONFIG));

    List<ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigs =
        configManager.getAllScopedAnomalyDetectionConfig(requestContext, filter);
    assertEquals(3, scopedAnomalyDetectionConfigs.size());

    assertEquals(
        customerScopeResolvedConfig, getConfig(customerConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(
        serviceScopeResolvedConfig, getConfig(serviceConfigScope, scopedAnomalyDetectionConfigs));
    assertEquals(apiScopeResolvedConfig, getConfig(apiConfigScope, scopedAnomalyDetectionConfigs));
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
}
