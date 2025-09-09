package ai.traceable.anomaly.config.service.global.status;

import static ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_HIGH;
import static ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_LOW;
import static ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel.ANOMALY_CONFIDENCE_LEVEL_MEDIUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.common.license.LicenseInfoLoader;
import ai.traceable.anomaly.config.service.common.license.LicenseMeteringServiceConfig;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConfig;
import ai.traceable.anomaly.config.service.global.AnomalyGlobalConfigServiceConstants;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionConfigType;
import ai.traceable.anomaly.config.service.v1.RuleVersionDataChange;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.global.ExcludedEventsGenerationConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalApiConfigChange;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfigChange;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ModsecGlobalConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.license.metering.service.api.v1.GetLicenseInfoRequest;
import ai.traceable.license.metering.service.api.v1.GetLicenseInfoResponse;
import ai.traceable.license.metering.service.api.v1.LicenseInfo;
import ai.traceable.license.metering.service.api.v1.LicenseMeteringServiceGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GlobalAnomalyConfigStatusManagerTest {

  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private static Channel channelForMockServer;
  private AnomalyGlobalConfigServiceConfig config;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private LicenseInfoLoader licenseInfoLoader;
  private ScopedGlobalConfigStatusChangeConverter configConverter;

  private GlobalAnomalyConfigStatusManagerImpl configStatusManager;

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

  @BeforeAll
  public static void setupServer() throws IOException {
    TestInterceptor testInterceptor = new TestInterceptor();

    String serverName = InProcessServerBuilder.generateName();
    mockServer =
        InProcessServerBuilder.forName(serverName)
            .addService(
                ServerInterceptors.intercept(new MockLicenseMeteringService(), testInterceptor))
            .build()
            .start();
    channelForMockServer = InProcessChannelBuilder.forName(serverName).directExecutor().build();
  }

  @BeforeEach
  public void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();

    configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    licenseInfoLoader =
        new LicenseInfoLoader(
            new LicenseMeteringServiceConfig(
                ConfigFactory.parseString(
                    "host = \"localhost\"\n"
                        + "  port = 51018\n"
                        + "  call.timeout.duration = 60000\n"
                        + "  cache.expiration.duration = 5m\n"
                        + "  cache.max.size = 5000")),
            LicenseMeteringServiceGrpc.newBlockingStub(channelForMockServer)
                .withCallCredentials(
                    RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider()
                        .get()));

    config =
        new AnomalyGlobalConfigServiceConfig(
            ConfigFactory.parseString(
                "disabled = true\n"
                    + "  internal = false\n"
                    + "  minConfidenceLevel = ANOMALY_CONFIDENCE_LEVEL_MEDIUM\n"
                    + "  modsecGlobalConfig.exitSpansEvalEnabled = false\n"
                    + "  modsecGlobalConfig.ruleVersion.newWebAppStableVersion = \"1.0.0\"\n"
                    + "  modsecGlobalConfig.ruleVersion.newWebAppStableVersionPublishedDate = \"2023-01-01T00:00:00Z\"\n"
                    + "  modsecGlobalConfig.ruleVersion.oldWebAppStableVersion = \"1.0.0\"\n"
                    + "  modsecGlobalConfig.ruleVersion.oldWebAppStableVersionPublishedDate = \"2023-01-01T00:00:00Z\"\n"
                    + "apiGlobalConfig.exitSpansEvalEnabled = false\n"
                    + "  globalGenAiConfig.disabled = true\n"
                    + "  licenseTiers = [\n"
                    + "    {\n"
                    + "        tier = TIER_TEAM_TRIAL\n"
                    + "        disabled = false\n"
                    + "    }\n"
                    + "  ]\n"));
    configConverter = new ScopedGlobalConfigStatusChangeConverter();
    this.configStatusManager =
        spy(
            new GlobalAnomalyConfigStatusManagerImpl(
                config,
                configServiceBlockingStub,
                configConverter,
                new AnomalyConfigScopeUtils(),
                licenseInfoLoader,
                mock(ConfigChangeEventGenerator.class)));
  }

  @AfterEach
  public void wrapup() {
    mockConfigService.shutdown();
  }

  @AfterAll
  public static void teardown() {
    mockServer.shutdownNow();
  }

  @Test
  public void test_getScopedAnomalyConfigStatus() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    AnomalyConfigStatusChange configStatusChange;
    AnomalyConfigStatus expectedCustomerStatus;
    AnomalyConfigStatus expectedEnvironmentStatus;
    AnomalyConfigStatus expectedServiceStatus;
    AnomalyConfigStatus expectedApiStatus;
    List<ScopedAnomalyConfigStatus> scopedConfigs;

    assertThrows(
        RuntimeException.class,
        () ->
            configStatusManager.getScopedAnomalyConfigStatus(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    {
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder()
              .setInternal(false)
              .setDisabled(true)
              .build(); // default status
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      scopedConfigs =
          configStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext, List.of());
      assertEquals(1, scopedConfigs.size());
      assertEquals(customerConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(ANOMALY_CONFIDENCE_LEVEL_MEDIUM, scopedConfigs.get(0).getMinConfidenceLevel());
    }
    {
      RequestContext teamTrialRequestContext =
          RequestContext.forTenantId(tenantId + "_" + LicenseInfo.Tier.TIER_TEAM_TRIAL);
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(teamTrialRequestContext, customerConfigScope)
              .getConfigStatus());
      scopedConfigs =
          configStatusManager.getAllScopedAnomalyConfigStatusConfigs(
              teamTrialRequestContext, List.of());
      assertEquals(1, scopedConfigs.size());
      assertEquals(customerConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(ANOMALY_CONFIDENCE_LEVEL_MEDIUM, scopedConfigs.get(0).getMinConfidenceLevel());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
      ScopedAnomalyConfigStatusChange customerScopedConfig =
          upsertCustomerConfigStatus(configStatusChange, tenantId);
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      ScopedAnomalyConfigStatus scopedAnomalyConfigStatus;

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          customerScopedConfig.getMinConfidenceLevel(),
          scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          customerScopedConfig.getExcludedEventsConfig(),
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, environmentConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          customerScopedConfig.getMinConfidenceLevel(),
          scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          customerScopedConfig.getExcludedEventsConfig(),
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, serviceConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          customerScopedConfig.getMinConfidenceLevel(),
          scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          customerScopedConfig.getExcludedEventsConfig(),
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, apiConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          customerScopedConfig.getMinConfidenceLevel(),
          scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          customerScopedConfig.getExcludedEventsConfig(),
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedConfigs =
          configStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext, List.of());
      assertEquals(1, scopedConfigs.size());
      assertEquals(customerConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(
          customerScopedConfig.getMinConfidenceLevel(),
          scopedConfigs.get(0).getMinConfidenceLevel());
      assertEquals(
          customerScopedConfig.getExcludedEventsConfig(),
          scopedConfigs.get(0).getExcludedEventsConfig());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
      ScopedAnomalyConfigStatusChange apiScopedConfig = upsertApiConfigStatus(configStatusChange);
      expectedApiStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(false).build();
      ScopedAnomalyConfigStatus scopedAnomalyConfigStatus;

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, apiConfigScope);
      assertEquals(expectedApiStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          apiScopedConfig.getMinConfidenceLevel(),
          scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          apiScopedConfig.getExcludedEventsConfig(),
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      // customer config stays unchanged..
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      ExcludedEventsGenerationConfig expectedCustomerExcludedEventsConfig =
          ExcludedEventsGenerationConfig.newBuilder().setEnabledForAll(true).build();
      AnomalyConfidenceLevel expectedCustomerMinConfidenceLevel = ANOMALY_CONFIDENCE_LEVEL_HIGH;

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          expectedCustomerMinConfidenceLevel, scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          expectedCustomerExcludedEventsConfig,
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, environmentConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          expectedCustomerMinConfidenceLevel, scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          expectedCustomerExcludedEventsConfig,
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedAnomalyConfigStatus =
          configStatusManager.getScopedAnomalyConfigStatus(requestContext, serviceConfigScope);
      assertEquals(expectedCustomerStatus, scopedAnomalyConfigStatus.getConfigStatus());
      assertEquals(
          expectedCustomerMinConfidenceLevel, scopedAnomalyConfigStatus.getMinConfidenceLevel());
      assertEquals(
          expectedCustomerExcludedEventsConfig,
          scopedAnomalyConfigStatus.getExcludedEventsConfig());

      scopedConfigs =
          configStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext, List.of());
      assertEquals(2, scopedConfigs.size());
      assertEquals(apiConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedApiStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(
          apiScopedConfig.getMinConfidenceLevel(), scopedConfigs.get(0).getMinConfidenceLevel());
      assertEquals(
          apiScopedConfig.getExcludedEventsConfig(),
          scopedConfigs.get(0).getExcludedEventsConfig());
      assertEquals(customerConfigScope, scopedConfigs.get(1).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(1).getConfigStatus());
      assertEquals(
          expectedCustomerMinConfidenceLevel, scopedConfigs.get(1).getMinConfidenceLevel());
      assertEquals(
          expectedCustomerExcludedEventsConfig, scopedConfigs.get(1).getExcludedEventsConfig());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
      upsertEnvironmentConfigStatus(configStatusChange);
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      expectedEnvironmentStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedEnvironmentStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, environmentConfigScope)
              .getConfigStatus());
      scopedConfigs =
          configStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext, List.of());
      assertEquals(3, scopedConfigs.size());
      assertEquals(environmentConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedEnvironmentStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(apiConfigScope, scopedConfigs.get(1).getConfigScope());
      assertEquals(expectedApiStatus, scopedConfigs.get(1).getConfigStatus());
      assertEquals(customerConfigScope, scopedConfigs.get(2).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(2).getConfigStatus());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
      upsertServiceConfigStatus(configStatusChange);
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(false).build();
      expectedServiceStatus =
          AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
      expectedApiStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedServiceStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, serviceConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedApiStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, apiConfigScope)
              .getConfigStatus());
      scopedConfigs =
          configStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext, List.of());
      assertEquals(4, scopedConfigs.size());
      assertEquals(environmentConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedEnvironmentStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(serviceConfigScope, scopedConfigs.get(1).getConfigScope());
      assertEquals(expectedServiceStatus, scopedConfigs.get(1).getConfigStatus());
      assertEquals(apiConfigScope, scopedConfigs.get(2).getConfigScope());
      assertEquals(expectedApiStatus, scopedConfigs.get(2).getConfigStatus());
      assertEquals(customerConfigScope, scopedConfigs.get(3).getConfigScope());
      assertEquals(expectedCustomerStatus, scopedConfigs.get(3).getConfigStatus());
    }
  }

  @Test
  void test_getUnresolvedScopedAnomalyConfigStatus() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    AnomalyConfigStatusChange configStatusChange;
    AnomalyConfigStatusChange expectedCustomerStatus;
    AnomalyConfigStatusChange expectedEnvironmentStatus;
    AnomalyConfigStatusChange expectedServiceStatus;
    AnomalyConfigStatusChange expectedApiStatus;
    List<ScopedAnomalyConfigStatusChange> scopedConfigs;

    assertThrows(
        RuntimeException.class,
        () ->
            configStatusManager.getUnresolvedScopedAnomalyConfigStatus(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    {
      expectedCustomerStatus = AnomalyConfigStatusChange.getDefaultInstance();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getUnresolvedScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      scopedConfigs =
          configStatusManager.getAllUnresolvedScopedAnomalyConfigStatusConfigs(
              requestContext, Collections.emptyList());
      assertEquals(0, scopedConfigs.size());
    }
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
      upsertEnvironmentConfigStatus(configStatusChange);
      expectedCustomerStatus = AnomalyConfigStatusChange.getDefaultInstance();
      expectedEnvironmentStatus = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getUnresolvedScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedEnvironmentStatus,
          configStatusManager
              .getUnresolvedScopedAnomalyConfigStatus(requestContext, environmentConfigScope)
              .getConfigStatus());
      scopedConfigs =
          configStatusManager.getAllUnresolvedScopedAnomalyConfigStatusConfigs(
              requestContext, Collections.emptyList());
      assertEquals(1, scopedConfigs.size());
      assertEquals(environmentConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedEnvironmentStatus, scopedConfigs.get(0).getConfigStatus());
    }
    {
      configStatusChange =
          AnomalyConfigStatusChange.newBuilder().setInternal(true).setDisabled(false).build();
      upsertServiceConfigStatus(configStatusChange);
      configStatusChange =
          AnomalyConfigStatusChange.newBuilder().setInternal(true).setDisabled(true).build();
      upsertApiConfigStatus(configStatusChange);
      expectedCustomerStatus = AnomalyConfigStatusChange.getDefaultInstance();
      expectedEnvironmentStatus = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
      expectedServiceStatus =
          AnomalyConfigStatusChange.newBuilder().setInternal(true).setDisabled(false).build();
      expectedApiStatus =
          AnomalyConfigStatusChange.newBuilder().setInternal(true).setDisabled(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getUnresolvedScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedEnvironmentStatus,
          configStatusManager
              .getUnresolvedScopedAnomalyConfigStatus(requestContext, environmentConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedServiceStatus,
          configStatusManager
              .getUnresolvedScopedAnomalyConfigStatus(requestContext, serviceConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedApiStatus,
          configStatusManager
              .getUnresolvedScopedAnomalyConfigStatus(requestContext, apiConfigScope)
              .getConfigStatus());
      scopedConfigs =
          configStatusManager.getAllUnresolvedScopedAnomalyConfigStatusConfigs(
              requestContext, Collections.emptyList());
      assertEquals(3, scopedConfigs.size());
      assertEquals(environmentConfigScope, scopedConfigs.get(0).getConfigScope());
      assertEquals(expectedEnvironmentStatus, scopedConfigs.get(0).getConfigStatus());
      assertEquals(serviceConfigScope, scopedConfigs.get(1).getConfigScope());
      assertEquals(expectedServiceStatus, scopedConfigs.get(1).getConfigStatus());
      assertEquals(apiConfigScope, scopedConfigs.get(2).getConfigScope());
    }
  }

  @Test
  void test_updateVersionSpecificAnomalyGlobalConfig_Api() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyConfigStatusChange existingConfig =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setGlobalApiConfigChange(
                GlobalApiConfigChange.newBuilder()
                    .setRuleVersionDataChange(
                        RuleVersionDataChange.newBuilder()
                            .setOverrideVersion(
                                RuleVersion.newBuilder().setVersion("1.1.0").build())
                            .build())
                    .build())
            .setConfigScope(customerConfigScope)
            .build();

    updateAnomalyConfigStatus(requestContext, existingConfig);

    ScopedAnomalyConfigStatusChange response =
        deleteRuleVersionConfigType(
            requestContext,
            List.of(RuleVersionConfigType.RULE_VERSION_CONFIG_TYPE_OVERRIDE),
            RuleType.RULE_TYPE_API_PROTECTION);

    assertFalse(
        response.getGlobalApiConfigChange().getRuleVersionDataChange().hasOverrideVersion());

    updateAnomalyConfigStatus(requestContext, existingConfig);
    response =
        deleteRuleVersionConfigType(
            requestContext,
            List.of(
                RuleVersionConfigType.RULE_VERSION_CONFIG_TYPE_OVERRIDE,
                RuleVersionConfigType.RULE_VERSION_CONFIG_TYPE_EXPERIMENTAL),
            RuleType.RULE_TYPE_API_PROTECTION);

    assertFalse(
        response.getGlobalApiConfigChange().getRuleVersionDataChange().hasOverrideVersion());
  }

  @Test
  void test_updateVersionSpecificAnomalyGlobalConfig_WebApp() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ScopedAnomalyConfigStatusChange request =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setGlobalModsecConfigChange(
                GlobalModsecConfigChange.newBuilder()
                    .setRuleVersionDataChange(
                        RuleVersionDataChange.newBuilder()
                            .setOverrideVersion(
                                RuleVersion.newBuilder().setVersion("1.1.0").build())
                            .setExperimentalVersion(
                                RuleVersion.newBuilder().setVersion("2.0.0").build())
                            .build())
                    .build())
            .setConfigScope(customerConfigScope)
            .build();

    updateAnomalyConfigStatus(requestContext, request);
    ScopedAnomalyConfigStatusChange response =
        deleteRuleVersionConfigType(
            requestContext,
            List.of(
                RuleVersionConfigType.RULE_VERSION_CONFIG_TYPE_OVERRIDE,
                RuleVersionConfigType.RULE_VERSION_CONFIG_TYPE_EXPERIMENTAL),
            RuleType.RULE_TYPE_WEB_APPLICATION);
    assertFalse(
        response.getGlobalModsecConfigChange().getRuleVersionDataChange().hasOverrideVersion());
    assertFalse(
        response.getGlobalModsecConfigChange().getRuleVersionDataChange().hasExperimentalVersion());

    updateAnomalyConfigStatus(requestContext, request);
    response =
        deleteRuleVersionConfigType(
            requestContext,
            List.of(RuleVersionConfigType.RULE_VERSION_CONFIG_TYPE_EXPERIMENTAL),
            RuleType.RULE_TYPE_WEB_APPLICATION);

    assertTrue(
        response.getGlobalModsecConfigChange().getRuleVersionDataChange().hasOverrideVersion());
    assertEquals(
        "1.1.0",
        response
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getOverrideVersion()
            .getVersion());
    assertFalse(
        response.getGlobalModsecConfigChange().getRuleVersionDataChange().hasExperimentalVersion());

    updateAnomalyConfigStatus(requestContext, request);
    response =
        deleteRuleVersionConfigType(
            requestContext,
            List.of(RuleVersionConfigType.RULE_VERSION_CONFIG_TYPE_OVERRIDE),
            RuleType.RULE_TYPE_WEB_APPLICATION);

    assertFalse(
        response.getGlobalModsecConfigChange().getRuleVersionDataChange().hasOverrideVersion());
    assertTrue(
        response.getGlobalModsecConfigChange().getRuleVersionDataChange().hasExperimentalVersion());
    assertEquals(
        "2.0.0",
        response
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getExperimentalVersion()
            .getVersion());
  }

  @Test
  void test_getScopedAnomalyConfigStatus_WithVersionHandling() {
    RuleVersion overrideVersion =
        RuleVersion.newBuilder()
            .setVersion("custom-version")
            .setVersionType(RuleVersionType.RULE_VERSION_TYPE_STABLE)
            .build();

    GlobalModsecConfigChange modsecConfigChange =
        GlobalModsecConfigChange.newBuilder()
            .setRuleVersionDataChange(
                RuleVersionDataChange.newBuilder().setOverrideVersion(overrideVersion).build())
            .build();

    AnomalyConfigScope customScope =
        AnomalyConfigScope.newBuilder()
            .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
            .build();

    ScopedAnomalyConfigStatusChange configWithOverride =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigScope(customScope)
            .setConfigStatus(AnomalyConfigStatusChange.getDefaultInstance())
            .setGlobalModsecConfigChange(modsecConfigChange)
            .build();

    RequestContext requestContext = RequestContext.forTenantId("custom_version_tenant");
    requestContext.call(
        () ->
            configStatusManager.updateScopedAnomalyConfigStatus(
                requestContext, configWithOverride));
    ScopedAnomalyConfigStatus result =
        configStatusManager.getScopedAnomalyConfigStatus(requestContext, customScope);

    assertEquals(
        "custom-version",
        result.getGlobalModsecConfig().getRuleVersionData().getCurrentVersion().getVersion());
    AnomalyConfigScope defaultScope =
        AnomalyConfigScope.newBuilder()
            .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
            .build();

    RequestContext defaultRequestContext = RequestContext.forTenantId("default_config_tenant");
    ScopedAnomalyConfigStatus defaultResult =
        configStatusManager.getScopedAnomalyConfigStatus(defaultRequestContext, defaultScope);
    assertEquals(
        "1.0.0",
        defaultResult
            .getGlobalModsecConfig()
            .getRuleVersionData()
            .getCurrentVersion()
            .getVersion());

    assertEquals(
        RuleVersionType.RULE_VERSION_TYPE_STABLE,
        defaultResult
            .getGlobalModsecConfig()
            .getRuleVersionData()
            .getCurrentVersion()
            .getVersionType());
  }

  @Test
  public void test_defaultConfigsType_setFromEnvironmentScope() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);

    ScopedAnomalyConfigStatus response =
        configStatusManager.getScopedAnomalyConfigStatus(requestContext, environmentConfigScope);
    assertEquals(
        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_ENVIRONMENT,
        response.getGlobalModsecConfig().getDefaultConfigsType());
    assertEquals(
        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_ENVIRONMENT,
        response.getModsecGlobalConfig().getDefaultConfigsType());
    assertEquals(
        response.getGlobalModsecConfig().getDisabled(),
        response.getModsecGlobalConfig().getDisabled());
    assertEquals(
        response.getGlobalModsecConfig().getBlockingAvailableForRegularRules(),
        response.getModsecGlobalConfig().getBlockingAvailableForRegularRules());
    assertEquals(
        response.getGlobalModsecConfig().getUseTestRules(),
        response.getModsecGlobalConfig().getUseTestRules());
    assertEquals(
        response.getGlobalModsecConfig().getMinConfidenceLevel(),
        response.getModsecGlobalConfig().getMinConfidenceLevel());
    assertEquals(
        response.getGlobalModsecConfig().getModsecEvaluationEngineConfig(),
        response.getModsecGlobalConfig().getModsecEvaluationEngineConfig());

    updateAnomalyConfigStatus(
        requestContext,
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigScope(environmentConfigScope)
            .setGlobalModsecConfigChange(
                GlobalModsecConfigChange.newBuilder()
                    .setDefaultConfigsType(
                        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STRICT_BLOCKING)
                    .build())
            .build());

    response =
        configStatusManager.getScopedAnomalyConfigStatus(requestContext, environmentConfigScope);
    assertEquals(
        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STRICT_BLOCKING,
        response.getGlobalModsecConfig().getDefaultConfigsType());
    assertEquals(
        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STRICT_BLOCKING,
        response.getModsecGlobalConfig().getDefaultConfigsType());
    assertEquals(
        response.getGlobalModsecConfig().getDisabled(),
        response.getModsecGlobalConfig().getDisabled());
    assertEquals(
        response.getGlobalModsecConfig().getBlockingAvailableForRegularRules(),
        response.getModsecGlobalConfig().getBlockingAvailableForRegularRules());
    assertEquals(
        response.getGlobalModsecConfig().getUseTestRules(),
        response.getModsecGlobalConfig().getUseTestRules());
    assertEquals(
        response.getGlobalModsecConfig().getMinConfidenceLevel(),
        response.getModsecGlobalConfig().getMinConfidenceLevel());
    assertEquals(
        response.getGlobalModsecConfig().getModsecEvaluationEngineConfig(),
        response.getModsecGlobalConfig().getModsecEvaluationEngineConfig());
  }

  @Test
  public void test_updateAnomalyConfigStatus() {
    RequestContext requestContext = RequestContext.forTenantId("update_tenant");

    assertThrows(
        RuntimeException.class,
        () ->
            updateAnomalyConfigStatus(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                AnomalyConfigStatusChange.getDefaultInstance()));

    AnomalyConfigStatusChange configStatusChange;
    AnomalyConfigStatus expectedCustomerStatus;
    {
      configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(true).build();
      assertEquals(
          configStatusChange,
          updateAnomalyConfigStatus(requestContext, customerConfigScope, configStatusChange));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, environmentConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, serviceConfigScope)
              .getConfigStatus());
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, apiConfigScope)
              .getConfigStatus());

      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(false).build();
      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(true).build(),
          updateAnomalyConfigStatus(requestContext, customerConfigScope, configStatusChange));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setDisabled(false).setInternal(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());

      configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
      assertEquals(
          AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build(),
          updateAnomalyConfigStatus(requestContext, customerConfigScope, configStatusChange));
      expectedCustomerStatus =
          AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(true).build();
      assertEquals(
          expectedCustomerStatus,
          configStatusManager
              .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
              .getConfigStatus());
    }

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    assertEquals(
        configStatusChange,
        updateAnomalyConfigStatus(requestContext, environmentConfigScope, configStatusChange));
    AnomalyConfigStatus expectedEnvironmentStatus =
        AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
            .getConfigStatus());
    assertEquals(
        expectedEnvironmentStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, environmentConfigScope)
            .getConfigStatus());

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setDisabled(true).build();
    assertEquals(
        configStatusChange,
        updateAnomalyConfigStatus(requestContext, serviceConfigScope, configStatusChange));
    AnomalyConfigStatus expectedServiceStatus =
        AnomalyConfigStatus.newBuilder().setInternal(true).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
            .getConfigStatus());
    assertEquals(
        expectedServiceStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, serviceConfigScope)
            .getConfigStatus());
    assertEquals(
        expectedServiceStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, apiConfigScope)
            .getConfigStatus());

    configStatusChange = AnomalyConfigStatusChange.newBuilder().setInternal(false).build();
    assertEquals(
        configStatusChange,
        updateAnomalyConfigStatus(requestContext, apiConfigScope, configStatusChange));
    AnomalyConfigStatus expectedApiStatus =
        AnomalyConfigStatus.newBuilder().setInternal(false).setDisabled(true).build();
    assertEquals(
        expectedCustomerStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, customerConfigScope)
            .getConfigStatus());
    assertEquals(
        expectedServiceStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, serviceConfigScope)
            .getConfigStatus());
    assertEquals(
        expectedApiStatus,
        configStatusManager
            .getScopedAnomalyConfigStatus(requestContext, apiConfigScope)
            .getConfigStatus());
  }

  @Test
  void test_updateAnomalyConfigStatusNew() {
    ScopedAnomalyConfigStatusChange newConfig =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigScope(environmentConfigScope)
            .setGlobalModsecConfigChange(
                GlobalModsecConfigChange.newBuilder()
                    .setDisabled(true)
                    .setDefaultConfigsType(
                        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STRICT_BLOCKING)
                    .setBlockingAvailableForRegularRules(false)
                    .setUseTestRules(true)
                    .setMinConfidenceLevel(ANOMALY_CONFIDENCE_LEVEL_MEDIUM)
                    .build())
            .build();

    RequestContext requestContext = RequestContext.forTenantId("update_tenant");
    ScopedAnomalyConfigStatusChange result = updateAnomalyConfigStatus(requestContext, newConfig);

    assertEquals(true, result.hasModsecGlobalConfig());
    assertEquals(true, result.hasGlobalModsecConfigChange());

    ModsecGlobalConfig modsec = result.getModsecGlobalConfig();
    GlobalModsecConfigChange globalModsec = result.getGlobalModsecConfigChange();
    assertEquals(modsec.getDisabled(), globalModsec.getDisabled());
    assertEquals(modsec.getDefaultConfigsType(), globalModsec.getDefaultConfigsType());
    assertEquals(
        modsec.getBlockingAvailableForRegularRules(),
        globalModsec.getBlockingAvailableForRegularRules());
    assertEquals(modsec.getUseTestRules(), globalModsec.getUseTestRules());
    assertEquals(modsec.getMinConfidenceLevel(), globalModsec.getMinConfidenceLevel());
    assertEquals(true, modsec.getDisabled());
    assertEquals(
        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STRICT_BLOCKING,
        modsec.getDefaultConfigsType());
    assertEquals(false, modsec.getBlockingAvailableForRegularRules());
    assertEquals(true, modsec.getUseTestRules());
    assertEquals(ANOMALY_CONFIDENCE_LEVEL_MEDIUM, modsec.getMinConfidenceLevel());

    newConfig =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigScope(environmentConfigScope)
            .setModsecGlobalConfig(
                ModsecGlobalConfig.newBuilder()
                    .setDisabled(false)
                    .setDefaultConfigsType(
                        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD_BLOCKING)
                    .setBlockingAvailableForRegularRules(true)
                    .setUseTestRules(false)
                    .setMinConfidenceLevel(ANOMALY_CONFIDENCE_LEVEL_HIGH)
                    .build())
            .build();
    result = updateAnomalyConfigStatus(requestContext, newConfig);
    assertEquals(true, result.hasModsecGlobalConfig());
    assertEquals(true, result.hasGlobalModsecConfigChange());
    modsec = result.getModsecGlobalConfig();
    assertEquals(false, modsec.getDisabled());
    assertEquals(
        ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_STANDARD_BLOCKING,
        modsec.getDefaultConfigsType());
    assertEquals(true, modsec.getBlockingAvailableForRegularRules());
    assertEquals(false, modsec.getUseTestRules());
    assertEquals(ANOMALY_CONFIDENCE_LEVEL_HIGH, modsec.getMinConfidenceLevel());
  }

  @Test
  void test_sendNotificationOnConfigUpdate() {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    AnomalyConfigScope customerScope =
        AnomalyConfigScope.newBuilder()
            .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
            .build();
    requestContext.call(
        () -> configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerScope));

    ScopedAnomalyConfigStatusChange unresolvedScopedAnomalyConfigStatusResult =
        configStatusManager.getUnresolvedScopedAnomalyConfigStatus(requestContext, customerScope);
    assertFalse(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .hasNotificationConfig());

    String today = ZonedDateTime.now(ZoneOffset.UTC).toString();
    String configStr =
        String.format(
            "disabled = true\n"
                + "internal = false\n"
                + "minConfidenceLevel = ANOMALY_CONFIDENCE_LEVEL_MEDIUM\n"
                + "modsecGlobalConfig.exitSpansEvalEnabled = false\n"
                + "modsecGlobalConfig.ruleVersion.newWebAppStableVersion = \"1.1.0\"\n"
                + "modsecGlobalConfig.ruleVersion.newWebAppStableVersionPublishedDate = \"%s\"\n"
                + "modsecGlobalConfig.ruleVersion.oldWebAppStableVersion = \"1.0.0\"\n"
                + "modsecGlobalConfig.ruleVersion.oldWebAppStableVersionPublishedDate = \"2023-01-01T00:00:00Z\"\n"
                + "apiGlobalConfig.exitSpansEvalEnabled = false\n"
                + "globalGenAiConfig.disabled = true\n"
                + "licenseTiers = [\n"
                + "    {\n"
                + "        tier = TIER_TEAM_TRIAL\n"
                + "        disabled = false\n"
                + "    }\n"
                + "]\n",
            today);
    config = new AnomalyGlobalConfigServiceConfig(ConfigFactory.parseString(configStr));
    this.configStatusManager =
        spy(
            new GlobalAnomalyConfigStatusManagerImpl(
                config,
                configServiceBlockingStub,
                configConverter,
                new AnomalyConfigScopeUtils(),
                licenseInfoLoader,
                mock(ConfigChangeEventGenerator.class)));
    requestContext.call(
        () -> configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerScope));
    unresolvedScopedAnomalyConfigStatusResult =
        configStatusManager.getUnresolvedScopedAnomalyConfigStatus(requestContext, customerScope);
    assertTrue(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .hasNotificationConfig());
    assertTrue(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getNotificationConfig()
            .getReleaseNotified());
    assertFalse(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getNotificationConfig()
            .getExpiryNotified());
    assertFalse(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getNotificationConfig()
            .getExpiryWarningNotified());

    // once released Notification sent then it should not be sent again
    requestContext.call(
        () -> configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerScope));
    ScopedAnomalyConfigStatusChange unresolvedScopedAnomalyConfigStatusResult2 =
        configStatusManager.getUnresolvedScopedAnomalyConfigStatus(requestContext, customerScope);
    assertEquals(
        unresolvedScopedAnomalyConfigStatusResult, unresolvedScopedAnomalyConfigStatusResult2);

    // after 13 days, the notification should be sent again
    ZonedDateTime futureDate = ZonedDateTime.now(ZoneOffset.UTC).minusDays(13);
    configStr =
        String.format(
            "disabled = true\n"
                + "internal = false\n"
                + "minConfidenceLevel = ANOMALY_CONFIDENCE_LEVEL_MEDIUM\n"
                + "modsecGlobalConfig.exitSpansEvalEnabled = false\n"
                + "modsecGlobalConfig.ruleVersion.newWebAppStableVersion = \"1.1.0\"\n"
                + "modsecGlobalConfig.ruleVersion.newWebAppStableVersionPublishedDate = \"%s\"\n"
                + "modsecGlobalConfig.ruleVersion.oldWebAppStableVersion = \"1.0.0\"\n"
                + "modsecGlobalConfig.ruleVersion.oldWebAppStableVersionPublishedDate = \"2023-01-01T00:00:00Z\"\n"
                + "apiGlobalConfig.exitSpansEvalEnabled = false\n"
                + "globalGenAiConfig.disabled = true\n"
                + "licenseTiers = [\n"
                + "    {\n"
                + "        tier = TIER_TEAM_TRIAL\n"
                + "        disabled = false\n"
                + "    }\n"
                + "]\n",
            futureDate);
    config = new AnomalyGlobalConfigServiceConfig(ConfigFactory.parseString(configStr));
    this.configStatusManager =
        spy(
            new GlobalAnomalyConfigStatusManagerImpl(
                config,
                configServiceBlockingStub,
                configConverter,
                new AnomalyConfigScopeUtils(),
                licenseInfoLoader,
                mock(ConfigChangeEventGenerator.class)));
    requestContext.call(
        () -> configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerScope));
    unresolvedScopedAnomalyConfigStatusResult =
        configStatusManager.getUnresolvedScopedAnomalyConfigStatus(requestContext, customerScope);
    assertTrue(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .hasNotificationConfig());
    assertFalse(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getNotificationConfig()
            .getReleaseNotified());
    assertFalse(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getNotificationConfig()
            .getExpiryNotified());
    assertTrue(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getNotificationConfig()
            .getExpiryWarningNotified());

    // after 14 days, the notification should be sent again
    futureDate = ZonedDateTime.now(ZoneOffset.UTC).minusDays(14);
    configStr =
        String.format(
            "disabled = true\n"
                + "internal = false\n"
                + "minConfidenceLevel = ANOMALY_CONFIDENCE_LEVEL_MEDIUM\n"
                + "modsecGlobalConfig.exitSpansEvalEnabled = false\n"
                + "modsecGlobalConfig.ruleVersion.newWebAppStableVersion = \"1.1.0\"\n"
                + "modsecGlobalConfig.ruleVersion.newWebAppStableVersionPublishedDate = \"%s\"\n"
                + "modsecGlobalConfig.ruleVersion.oldWebAppStableVersion = \"1.0.0\"\n"
                + "modsecGlobalConfig.ruleVersion.oldWebAppStableVersionPublishedDate = \"2023-01-01T00:00:00Z\"\n"
                + "apiGlobalConfig.exitSpansEvalEnabled = false\n"
                + "globalGenAiConfig.disabled = true\n"
                + "licenseTiers = [\n"
                + "    {\n"
                + "        tier = TIER_TEAM_TRIAL\n"
                + "        disabled = false\n"
                + "    }\n"
                + "]\n",
            futureDate);
    config = new AnomalyGlobalConfigServiceConfig(ConfigFactory.parseString(configStr));
    this.configStatusManager =
        spy(
            new GlobalAnomalyConfigStatusManagerImpl(
                config,
                configServiceBlockingStub,
                configConverter,
                new AnomalyConfigScopeUtils(),
                licenseInfoLoader,
                mock(ConfigChangeEventGenerator.class)));
    requestContext.call(
        () -> configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerScope));
    unresolvedScopedAnomalyConfigStatusResult =
        configStatusManager.getUnresolvedScopedAnomalyConfigStatus(requestContext, customerScope);
    assertTrue(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .hasNotificationConfig());
    assertFalse(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getNotificationConfig()
            .getReleaseNotified());
    assertTrue(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getNotificationConfig()
            .getExpiryNotified());
    assertFalse(
        unresolvedScopedAnomalyConfigStatusResult
            .getGlobalModsecConfigChange()
            .getRuleVersionDataChange()
            .getNotificationConfig()
            .getExpiryWarningNotified());

    // after this there should be no notification sent
    futureDate = ZonedDateTime.now(ZoneOffset.UTC).minusDays(15);
    configStr =
        String.format(
            "disabled = true\n"
                + "internal = false\n"
                + "minConfidenceLevel = ANOMALY_CONFIDENCE_LEVEL_MEDIUM\n"
                + "modsecGlobalConfig.exitSpansEvalEnabled = false\n"
                + "modsecGlobalConfig.ruleVersion.newWebAppStableVersion = \"1.1.0\"\n"
                + "modsecGlobalConfig.ruleVersion.newWebAppStableVersionPublishedDate = \"%s\"\n"
                + "modsecGlobalConfig.ruleVersion.oldWebAppStableVersion = \"1.0.0\"\n"
                + "modsecGlobalConfig.ruleVersion.oldWebAppStableVersionPublishedDate = \"2023-01-01T00:00:00Z\"\n"
                + "apiGlobalConfig.exitSpansEvalEnabled = false\n"
                + "globalGenAiConfig.disabled = true\n"
                + "licenseTiers = [\n"
                + "    {\n"
                + "        tier = TIER_TEAM_TRIAL\n"
                + "        disabled = false\n"
                + "    }\n"
                + "]\n",
            futureDate);
    config = new AnomalyGlobalConfigServiceConfig(ConfigFactory.parseString(configStr));
    this.configStatusManager =
        spy(
            new GlobalAnomalyConfigStatusManagerImpl(
                config,
                configServiceBlockingStub,
                configConverter,
                new AnomalyConfigScopeUtils(),
                licenseInfoLoader,
                mock(ConfigChangeEventGenerator.class)));
    requestContext.call(
        () -> configStatusManager.getScopedAnomalyConfigStatus(requestContext, customerScope));
    unresolvedScopedAnomalyConfigStatusResult2 =
        configStatusManager.getUnresolvedScopedAnomalyConfigStatus(requestContext, customerScope);
    assertEquals(
        unresolvedScopedAnomalyConfigStatusResult, unresolvedScopedAnomalyConfigStatusResult2);
  }

  private ScopedAnomalyConfigStatusChange upsertApiConfigStatus(
      AnomalyConfigStatusChange configStatus) throws InvalidProtocolBufferException {
    ScopedAnomalyConfigStatusChange scopedConfig =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigStatus(configStatus)
            .setConfigScope(apiConfigScope)
            .setExcludedEventsConfig(
                ExcludedEventsGenerationConfig.newBuilder()
                    .setExclusionRuleIds(
                        StringList.newBuilder().addAllValues(List.of("rule1", "rule2"))))
            .setMinConfidenceLevel(ANOMALY_CONFIDENCE_LEVEL_LOW)
            .build();
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_NAMESPACE)
            .setResourceName(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_STATUS_RESOURCE_NAME)
            .setConfig(configConverter.convert(scopedConfig))
            .setContext(apiScope.getId())
            .build());
    return scopedConfig;
  }

  private void upsertEnvironmentConfigStatus(AnomalyConfigStatusChange configStatus)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_NAMESPACE)
            .setResourceName(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_STATUS_RESOURCE_NAME)
            .setConfig(
                configConverter.convert(
                    ScopedAnomalyConfigStatusChange.newBuilder()
                        .setConfigStatus(configStatus)
                        .setConfigScope(environmentConfigScope)
                        .build()))
            .setContext(environmentScope.getEnvironmentId())
            .build());
  }

  private void upsertServiceConfigStatus(AnomalyConfigStatusChange configStatus)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_NAMESPACE)
            .setResourceName(
                AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_STATUS_RESOURCE_NAME)
            .setConfig(
                configConverter.convert(
                    ScopedAnomalyConfigStatusChange.newBuilder()
                        .setConfigStatus(configStatus)
                        .setConfigScope(serviceConfigScope)
                        .build()))
            .setContext(serviceScope.getId())
            .build());
  }

  private ScopedAnomalyConfigStatusChange upsertCustomerConfigStatus(
      AnomalyConfigStatusChange configStatus, String tenantId)
      throws InvalidProtocolBufferException {
    ScopedAnomalyConfigStatusChange scopedConfig =
        ScopedAnomalyConfigStatusChange.newBuilder()
            .setConfigStatus(configStatus)
            .setConfigScope(customerConfigScope)
            .setExcludedEventsConfig(
                ExcludedEventsGenerationConfig.newBuilder().setEnabledForAll(true))
            .setMinConfidenceLevel(ANOMALY_CONFIDENCE_LEVEL_HIGH)
            .build();
    RequestContext.forTenantId(tenantId)
        .call(
            () ->
                configServiceBlockingStub.upsertConfig(
                    UpsertConfigRequest.newBuilder()
                        .setContext(tenantId)
                        .setResourceNamespace(
                            AnomalyGlobalConfigServiceConstants.GLOBAL_ANOMALY_CONFIG_NAMESPACE)
                        .setResourceName(
                            AnomalyGlobalConfigServiceConstants
                                .GLOBAL_ANOMALY_CONFIG_STATUS_RESOURCE_NAME)
                        .setConfig(configConverter.convert(scopedConfig))
                        .build()));
    return scopedConfig;
  }

  private AnomalyConfigStatusChange updateAnomalyConfigStatus(
      RequestContext requestContext, AnomalyConfigScope scope, AnomalyConfigStatusChange status) {
    ScopedAnomalyConfigStatusChange scopedConfig =
        requestContext.call(
            () ->
                configStatusManager.updateScopedAnomalyConfigStatus(
                    requestContext,
                    ScopedAnomalyConfigStatusChange.newBuilder()
                        .setConfigScope(scope)
                        .setConfigStatus(status)
                        .build()));
    assertEquals(scope, scopedConfig.getConfigScope());
    return scopedConfig.getConfigStatus();
  }

  private ScopedAnomalyConfigStatusChange updateAnomalyConfigStatus(
      RequestContext requestContext, ScopedAnomalyConfigStatusChange scopedConfig) {
    ScopedAnomalyConfigStatusChange result =
        requestContext.call(
            () ->
                configStatusManager.updateScopedAnomalyConfigStatus(requestContext, scopedConfig));
    return result;
  }

  private ScopedAnomalyConfigStatusChange deleteRuleVersionConfigType(
      RequestContext requestContext,
      List<RuleVersionConfigType> ruleVersionConfigTypes,
      RuleType ruleType) {
    ScopedAnomalyConfigStatusChange result =
        requestContext.call(
            () ->
                requestContext.call(
                    () ->
                        configStatusManager.deleteRuleVersionConfigType(
                            requestContext,
                            customerConfigScope,
                            ruleVersionConfigTypes,
                            ruleType)));
    return result;
  }

  protected static class MockLicenseMeteringService
      extends LicenseMeteringServiceGrpc.LicenseMeteringServiceImplBase {
    private static final String TENANT_ID_PREFIX = "tenant_";
    private static final int TENANT_ID_PREFIX_LENGTH = TENANT_ID_PREFIX.length();

    @Override
    public void getLicenseInfo(
        GetLicenseInfoRequest request,
        StreamObserver<GetLicenseInfoResponse> responseStreamObserver) {
      String tenantId = RequestContext.CURRENT.get().getTenantId().get();
      if (tenantId.startsWith(TENANT_ID_PREFIX)) {
        responseStreamObserver.onNext(
            GetLicenseInfoResponse.newBuilder()
                .setLicenseInfo(
                    LicenseInfo.newBuilder()
                        .setTier(
                            LicenseInfo.Tier.valueOf(tenantId.substring(TENANT_ID_PREFIX_LENGTH))))
                .build());
        responseStreamObserver.onCompleted();
        return;
      }
      responseStreamObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  private static class TestInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      Context ctx =
          Context.current()
              .withValue(
                  RequestContext.CURRENT,
                  RequestContext.forTenantId(
                      headers.get(
                          Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER))));
      return Contexts.interceptCall(ctx, call, headers, next);
    }
  }
}
