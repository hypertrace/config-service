package ai.traceable.threatmanagement.config.service.statuscode;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.threatmanagement.config.service.v1.Protocol;
import ai.traceable.threatmanagement.config.service.v1.SeverityDowngradePolicy;
import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfigs;
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

class StatusCodeThreatScoreConfigsManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private static Channel channelForMockServer;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private StatusCodeThreatScoreConfigsManager statusCodeThreatScoreConfigsManager;

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
    statusCodeThreatScoreConfigsManager =
        new StatusCodeThreatScoreConfigsManagerImpl(configServiceBlockingStub);
  }

  @AfterEach
  public void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdown();
  }

  @Test
  void testGetAndUpdateStatusCodeThreatScoreConfigs() {
    StatusCodeThreatScoreConfigs statusCodeThreatScoreConfigs;
    statusCodeThreatScoreConfigs =
        REQUEST_CONTEXT.call(
            () ->
                statusCodeThreatScoreConfigsManager.getStatusCodeThreatScoreConfigs(
                    REQUEST_CONTEXT));
    assertEquals(List.of(), statusCodeThreatScoreConfigs.getConfigsList());

    StatusCodeThreatScoreConfigs configsToUpsert1 =
        StatusCodeThreatScoreConfigs.newBuilder()
            .addAllConfigs(
                List.of(
                    getStatusCodeThreatScoreConfig(
                        Protocol.PROTOCOL_HTTP,
                        "403",
                        SeverityDowngradePolicy.SEVERITY_DOWNGRADE_POLICY_ONE_STEP),
                    getStatusCodeThreatScoreConfig(
                        Protocol.PROTOCOL_GRPC,
                        "2",
                        SeverityDowngradePolicy.SEVERITY_DOWNGRADE_POLICY_IGNORE_SCORE)))
            .build();
    statusCodeThreatScoreConfigs =
        REQUEST_CONTEXT.call(
            () ->
                statusCodeThreatScoreConfigsManager.updateStatusCodeThreatScoreConfigs(
                    REQUEST_CONTEXT, configsToUpsert1));
    assertEquals(configsToUpsert1, statusCodeThreatScoreConfigs);

    statusCodeThreatScoreConfigs =
        REQUEST_CONTEXT.call(
            () ->
                statusCodeThreatScoreConfigsManager.getStatusCodeThreatScoreConfigs(
                    REQUEST_CONTEXT));
    assertEquals(configsToUpsert1, statusCodeThreatScoreConfigs);

    // update request config replaces existing one
    StatusCodeThreatScoreConfigs configsToUpsert2 =
        StatusCodeThreatScoreConfigs.newBuilder()
            .addAllConfigs(
                List.of(
                    getStatusCodeThreatScoreConfig(
                        Protocol.PROTOCOL_HTTP,
                        "400",
                        SeverityDowngradePolicy.SEVERITY_DOWNGRADE_POLICY_TWO_STEPS)))
            .build();
    statusCodeThreatScoreConfigs =
        REQUEST_CONTEXT.call(
            () ->
                statusCodeThreatScoreConfigsManager.updateStatusCodeThreatScoreConfigs(
                    REQUEST_CONTEXT, configsToUpsert2));
    assertEquals(configsToUpsert2, statusCodeThreatScoreConfigs);

    statusCodeThreatScoreConfigs =
        REQUEST_CONTEXT.call(
            () ->
                statusCodeThreatScoreConfigsManager.getStatusCodeThreatScoreConfigs(
                    REQUEST_CONTEXT));
    assertEquals(configsToUpsert2, statusCodeThreatScoreConfigs);
  }

  private StatusCodeThreatScoreConfig getStatusCodeThreatScoreConfig(
      Protocol protocol, String regex, SeverityDowngradePolicy downgradePolicy) {
    return StatusCodeThreatScoreConfig.newBuilder()
        .setProtocol(protocol)
        .setErrorStatusCodeRegex(regex)
        .setSeverityDowngradePolicy(downgradePolicy)
        .build();
  }
}
