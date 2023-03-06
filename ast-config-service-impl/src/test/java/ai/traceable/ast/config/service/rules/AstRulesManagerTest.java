package ai.traceable.ast.config.service.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import com.google.protobuf.Duration;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AstRulesManagerTest {
  private MockGenericConfigService mockConfigService;
  private RulesManager rulesManager;
  private RequestContext requestContext;

  @BeforeEach
  void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());

    ScanPurgeConfigStore configStore = new ScanPurgeConfigStore(configServiceBlockingStub);
    rulesManager = new AstRulesManager(configStore);
    requestContext = RequestContext.forTenantId("default-tenant");
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Test
  void testUpdateScanPurgeConfig() {
    ScanPurgeConfig purgeConfig =
        ScanPurgeConfig.newBuilder()
            .setPurgeDuration(Duration.newBuilder().setSeconds(123))
            .build();

    UpdateScanPurgeConfigRequest request =
        UpdateScanPurgeConfigRequest.newBuilder().setPurgeConfig(purgeConfig).build();

    ScanPurgeConfig updatedPurgeConfig =
        rulesManager.updateScanPurgeConfig(requestContext, request);
    assertEquals(
        purgeConfig.getPurgeDuration().getSeconds(),
        updatedPurgeConfig.getPurgeDuration().getSeconds());
  }

  @Test
  void testGetScanPurgeConfig() {
    ScanPurgeConfig purgeConfig =
        ScanPurgeConfig.newBuilder()
            .setPurgeDuration(Duration.newBuilder().setSeconds(123))
            .build();

    UpdateScanPurgeConfigRequest request =
        UpdateScanPurgeConfigRequest.newBuilder().setPurgeConfig(purgeConfig).build();

    rulesManager.updateScanPurgeConfig(requestContext, request);
    ScanPurgeConfig returnedPurgeConfig = rulesManager.getScanPurgeConfig(requestContext).get();

    assertEquals(
        purgeConfig.getPurgeDuration().getSeconds(),
        returnedPurgeConfig.getPurgeDuration().getSeconds());
  }
}
