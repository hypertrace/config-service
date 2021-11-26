package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfigType;
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

public class TrainingConfigManagerTest {

  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private static Channel channelForMockServer;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;

  private TrainingConfigConverter configConverter;
  private TrainingConfigManager configManager;

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

  private static final String TRAINING_CONFIG_DIRECTORY = "trainer/";
  private static final String SCOPED_TRAINING_CONFIGS_FILE_PATH =
      TRAINING_CONFIG_DIRECTORY + "scoped-training-configs.conf";
  private static final String RESOLVED_TRAINING_CONFIGS_FILE_PATH =
      TRAINING_CONFIG_DIRECTORY + "resolved-training-configs.conf";

  private static final String CUSTOMER_SCOPE_CONFIG = "customerScopeConfig";
  private static final String SERVICE_SCOPE_CONFIG = "serviceScopeConfig";
  private static final String API_SCOPE_CONFIG = "apiScopeConfig";

  private static final Config scopedTrainingConfigs =
      ConfigFactory.parseResources(SCOPED_TRAINING_CONFIGS_FILE_PATH);
  private static final Config resolvedTrainingConfigs =
      ConfigFactory.parseResources(RESOLVED_TRAINING_CONFIGS_FILE_PATH);

  @BeforeEach
  public void setup() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    channelForMockServer = InProcessChannelBuilder.forName(serverName).build();
    mockServer = InProcessServerBuilder.forName(serverName).build().start();
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    configConverter = new TrainingConfigConverter();
    this.configManager =
        spy(new TrainingConfigManagerImpl(configConverter, configServiceBlockingStub));
  }

  @AfterEach
  public void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdown();
  }

  @Test
  void testGetAndUpdateScopedTrainingConfig() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedTrainingConfig scopedTrainingConfig;

    assertThrows(
        RuntimeException.class,
        () ->
            configManager.getScopedTrainingConfig(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                GetTrainingConfigsFilter.getDefaultInstance()));

    scopedTrainingConfig =
        getScopedTrainingConfig(scopedTrainingConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    ScopedTrainingConfig customerScopeResolvedConfig =
        getScopedTrainingConfig(resolvedTrainingConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));

    configManager.updateScopedTrainingConfig(requestContext, scopedTrainingConfig);

    assertEquals(
        customerScopeResolvedConfig,
        configManager.getScopedTrainingConfig(
            requestContext, customerConfigScope, GetTrainingConfigsFilter.getDefaultInstance()));
    assertEquals(
        customerScopeResolvedConfig,
        configManager.getScopedTrainingConfig(
            requestContext, serviceConfigScope, GetTrainingConfigsFilter.getDefaultInstance()));
    assertEquals(
        customerScopeResolvedConfig,
        configManager.getScopedTrainingConfig(
            requestContext, apiConfigScope, GetTrainingConfigsFilter.getDefaultInstance()));

    scopedTrainingConfig =
        getScopedTrainingConfig(scopedTrainingConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    ScopedTrainingConfig serviceScopeResolvedConfig =
        getScopedTrainingConfig(resolvedTrainingConfigs.getConfig(SERVICE_SCOPE_CONFIG));

    configManager.updateScopedTrainingConfig(requestContext, scopedTrainingConfig);

    assertEquals(
        customerScopeResolvedConfig,
        configManager.getScopedTrainingConfig(
            requestContext, customerConfigScope, GetTrainingConfigsFilter.getDefaultInstance()));
    assertEquals(
        serviceScopeResolvedConfig,
        configManager.getScopedTrainingConfig(
            requestContext, serviceConfigScope, GetTrainingConfigsFilter.getDefaultInstance()));
    assertEquals(
        serviceScopeResolvedConfig,
        configManager.getScopedTrainingConfig(
            requestContext, apiConfigScope, GetTrainingConfigsFilter.getDefaultInstance()));

    scopedTrainingConfig =
        getScopedTrainingConfig(scopedTrainingConfigs.getConfig(API_SCOPE_CONFIG));
    ScopedTrainingConfig apiScopeResolvedConfig =
        getScopedTrainingConfig(resolvedTrainingConfigs.getConfig(API_SCOPE_CONFIG));

    configManager.updateScopedTrainingConfig(requestContext, scopedTrainingConfig);

    assertEquals(
        customerScopeResolvedConfig,
        configManager.getScopedTrainingConfig(
            requestContext, customerConfigScope, GetTrainingConfigsFilter.getDefaultInstance()));
    assertEquals(
        serviceScopeResolvedConfig,
        configManager.getScopedTrainingConfig(
            requestContext, serviceConfigScope, GetTrainingConfigsFilter.getDefaultInstance()));

    assertEquals(
        apiScopeResolvedConfig,
        configManager.getScopedTrainingConfig(
            requestContext, apiConfigScope, GetTrainingConfigsFilter.getDefaultInstance()));

    GetTrainingConfigsFilter filter =
        GetTrainingConfigsFilter.newBuilder()
            .addTrainingConfigTypes(TrainingConfigType.TRAINING_CONFIG_TYPE_VULNERABILITY)
            .addTrainingConfigTypes(TrainingConfigType.TRAINING_CONFIG_TYPE_METADATA)
            .addTrainingConfigTypes(TrainingConfigType.TRAINING_CONFIG_TYPE_SESSION)
            .build();

    assertEquals(
        500,
        configManager
            .getScopedTrainingConfig(requestContext, customerConfigScope, filter)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    filter =
        GetTrainingConfigsFilter.newBuilder()
            .addTrainingConfigTypes(TrainingConfigType.TRAINING_CONFIG_TYPE_METADATA)
            .build();
    assertEquals(
        List.of(),
        configManager
            .getScopedTrainingConfig(requestContext, customerConfigScope, filter)
            .getTrainingConfigsList());

    filter =
        GetTrainingConfigsFilter.newBuilder()
            .addTrainingConfigTypes(TrainingConfigType.TRAINING_CONFIG_TYPE_SESSION)
            .build();
    assertEquals(
        List.of(),
        configManager
            .getScopedTrainingConfig(requestContext, customerConfigScope, filter)
            .getTrainingConfigsList());
  }

  @Test
  void testGetAllScopedTrainingConfig() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedTrainingConfig scopedTrainingConfig;

    assertThrows(
        RuntimeException.class,
        () ->
            configManager.getScopedTrainingConfig(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                GetTrainingConfigsFilter.getDefaultInstance()));

    scopedTrainingConfig =
        getScopedTrainingConfig(scopedTrainingConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    configManager.updateScopedTrainingConfig(requestContext, scopedTrainingConfig);

    scopedTrainingConfig =
        getScopedTrainingConfig(scopedTrainingConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    configManager.updateScopedTrainingConfig(requestContext, scopedTrainingConfig);

    scopedTrainingConfig =
        getScopedTrainingConfig(scopedTrainingConfigs.getConfig(API_SCOPE_CONFIG));
    configManager.updateScopedTrainingConfig(requestContext, scopedTrainingConfig);

    ScopedTrainingConfig customerScopeResolvedConfig =
        getScopedTrainingConfig(resolvedTrainingConfigs.getConfig(CUSTOMER_SCOPE_CONFIG));
    ScopedTrainingConfig serviceScopeResolvedConfig =
        getScopedTrainingConfig(resolvedTrainingConfigs.getConfig(SERVICE_SCOPE_CONFIG));
    ScopedTrainingConfig apiScopeResolvedConfig =
        getScopedTrainingConfig(resolvedTrainingConfigs.getConfig(API_SCOPE_CONFIG));

    List<ScopedTrainingConfig> trainingConfigs =
        configManager.getAllScopedTrainingConfig(
            requestContext, GetTrainingConfigsFilter.getDefaultInstance());

    assertEquals(3, trainingConfigs.size());
    assertEquals(customerScopeResolvedConfig, getConfig(customerConfigScope, trainingConfigs));
    assertEquals(serviceScopeResolvedConfig, getConfig(serviceConfigScope, trainingConfigs));
    assertEquals(apiScopeResolvedConfig, getConfig(apiConfigScope, trainingConfigs));
  }

  private ScopedTrainingConfig getScopedTrainingConfig(Config config)
      throws InvalidProtocolBufferException {
    ScopedTrainingConfig.Builder builder = ScopedTrainingConfig.newBuilder();
    JsonFormat.parser()
        .ignoringUnknownFields()
        .merge(config.root().render(ConfigRenderOptions.concise()), builder);
    return builder.build();
  }

  private ScopedTrainingConfig getConfig(
      AnomalyConfigScope configScope, List<ScopedTrainingConfig> trainingConfigs) {

    for (ScopedTrainingConfig trainingConfig : trainingConfigs) {
      if (trainingConfig.getConfigScope().equals(configScope)) {
        return trainingConfig;
      }
    }
    return null;
  }
}
