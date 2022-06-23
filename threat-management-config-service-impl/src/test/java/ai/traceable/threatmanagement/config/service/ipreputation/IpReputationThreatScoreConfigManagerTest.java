package ai.traceable.threatmanagement.config.service.ipreputation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.IpReputationThreatScoreConfig;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IpReputationThreatScoreConfigManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private static Channel channelForMockServer;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private IpReputationThreatScoreConfigManager ipReputationThreatScoreConfigManager;
  private ThreatManagementConfigServiceConfig config;

  @BeforeEach
  void setUp() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    channelForMockServer = InProcessChannelBuilder.forName(serverName).build();
    mockServer = InProcessServerBuilder.forName(serverName).build().start();
    mockConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    config = mock(ThreatManagementConfigServiceConfig.class);
    ipReputationThreatScoreConfigManager =
        new IpReputationThreatScoreConfigManagerImpl(configServiceBlockingStub, config);
  }

  @AfterEach
  public void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdown();
  }

  @Test
  void testGetAndUpdateIpReputationThreatScoreConfigs() {
    when(config.getDefaultCriticalIpReputationThreatScoreIncrement()).thenReturn(3);
    when(config.getDefaultHighIpReputationThreatScoreIncrement()).thenReturn(2);
    when(config.getDefaultMediumIpReputationThreatScoreIncrement()).thenReturn(1);
    when(config.getDefaultLowIpReputationThreatScoreIncrement()).thenReturn(0);

    IpReputationThreatScoreConfig defaultIpReputationThreatScoreConfig =
        IpReputationThreatScoreConfig.newBuilder()
            .setCriticalIpReputationThreatScoreIncrement(3)
            .setHighIpReputationThreatScoreIncrement(2)
            .setMediumIpReputationThreatScoreIncrement(1)
            .build();

    IpReputationThreatScoreConfig ipReputationThreatScoreConfig;
    ipReputationThreatScoreConfig =
        REQUEST_CONTEXT.call(
            () ->
                ipReputationThreatScoreConfigManager.getIpReputationThreatScoreConfig(
                    REQUEST_CONTEXT));
    assertEquals(defaultIpReputationThreatScoreConfig, ipReputationThreatScoreConfig);

    IpReputationThreatScoreConfig configToUpsert1 =
        IpReputationThreatScoreConfig.newBuilder()
            .setCriticalIpReputationThreatScoreIncrement(6)
            .setHighIpReputationThreatScoreIncrement(4)
            .setMediumIpReputationThreatScoreIncrement(3)
            .setLowIpReputationThreatScoreIncrement(1)
            .build();
    ipReputationThreatScoreConfig =
        REQUEST_CONTEXT.call(
            () ->
                ipReputationThreatScoreConfigManager.updateIpReputationThreatScoreConfig(
                    REQUEST_CONTEXT, configToUpsert1));
    assertEquals(configToUpsert1, ipReputationThreatScoreConfig);

    ipReputationThreatScoreConfig =
        REQUEST_CONTEXT.call(
            () ->
                ipReputationThreatScoreConfigManager.getIpReputationThreatScoreConfig(
                    REQUEST_CONTEXT));
    assertEquals(configToUpsert1, ipReputationThreatScoreConfig);

    // update request config replaces existing one
    IpReputationThreatScoreConfig configToUpsert2 =
        IpReputationThreatScoreConfig.newBuilder()
            .setCriticalIpReputationThreatScoreIncrement(9)
            .setHighIpReputationThreatScoreIncrement(8)
            .setMediumIpReputationThreatScoreIncrement(3)
            .setLowIpReputationThreatScoreIncrement(2)
            .build();
    ipReputationThreatScoreConfig =
        REQUEST_CONTEXT.call(
            () ->
                ipReputationThreatScoreConfigManager.updateIpReputationThreatScoreConfig(
                    REQUEST_CONTEXT, configToUpsert2));
    assertEquals(configToUpsert2, ipReputationThreatScoreConfig);

    ipReputationThreatScoreConfig =
        REQUEST_CONTEXT.call(
            () ->
                ipReputationThreatScoreConfigManager.getIpReputationThreatScoreConfig(
                    REQUEST_CONTEXT));
    assertEquals(configToUpsert2, ipReputationThreatScoreConfig);

    ipReputationThreatScoreConfig =
        REQUEST_CONTEXT.call(
            () -> ipReputationThreatScoreConfigManager.getDefaultIpReputationThreatScoreConfig());
    assertEquals(defaultIpReputationThreatScoreConfig, ipReputationThreatScoreConfig);
  }
}
