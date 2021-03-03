package ai.traceable.localprocessing.config.service;

import static ai.traceable.localprocessing.config.service.LocalProcessingConfigServiceImpl.DEFAULT_PROTECTION_MODE;
import static ai.traceable.localprocessing.config.service.LocalProcessingConfigServiceImpl.LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.GetLocalProcessingConfigRequest;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingConfigServiceGrpc.LocalProcessingConfigServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc.LocalProcessingRulesServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectedEndpoint;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.ProtectionModeConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import java.util.Map;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalProcessingConfigServiceImplTest {

  LocalProcessingConfigServiceBlockingStub localProcessingConfigStub;
  LocalProcessingRulesServiceBlockingStub localProcessingRulesStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    Config config =
        ConfigFactory.parseMap(
            Map.of(
                LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG,
                Map.of(DEFAULT_PROTECTION_MODE, ProtectionMode.PROTECTION_MODE_ADVANCED.name())));
    Channel channel = mockGenericConfigService.channel();
    mockGenericConfigService
        .addService(new LocalProcessingConfigServiceImpl(channel, config))
        .addService(new LocalProcessingRulesServiceImpl(channel))
        .start();

    localProcessingConfigStub = LocalProcessingConfigServiceGrpc.newBlockingStub(channel);
    localProcessingRulesStub = LocalProcessingRulesServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void getLocalProcessingConfig() {
    createLocalProcessingRule("/checkout/*", "abc.com", ProtectionMode.PROTECTION_MODE_CORE);
    createLocalProcessingRule("/orders/**", "xyz.com", ProtectionMode.PROTECTION_MODE_ADVANCED);
    ProtectionModeConfig expectedConfig =
        ProtectionModeConfig.newBuilder()
            .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
            .addProtectedEndpoints(
                ProtectedEndpoint.newBuilder()
                    .setUrlPattern("/orders/**")
                    .setHostHeader("xyz.com")
                    .setProtectionMode(ProtectionMode.PROTECTION_MODE_ADVANCED)
                    .build())
            .addProtectedEndpoints(
                ProtectedEndpoint.newBuilder()
                    .setUrlPattern("/checkout/*")
                    .setHostHeader("abc.com")
                    .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
                    .build())
            .build();
    ProtectionModeConfig actualConfig =
        localProcessingConfigStub
            .getLocalProcessingConfig(GetLocalProcessingConfigRequest.getDefaultInstance())
            .getProtectionModeConfig();
    assertEquals(expectedConfig, actualConfig);
  }

  private LocalProcessingRuleDetails createLocalProcessingRule(
      String urlPattern, String hostHeader, ProtectionMode protectionMode) {
    NewLocalProcessingRule newLocalProcessingRule =
        NewLocalProcessingRule.newBuilder()
            .setUrlPattern(urlPattern)
            .setHostHeader(hostHeader)
            .setProtectionMode(protectionMode)
            .build();
    return localProcessingRulesStub
        .createLocalProcessingRule(
            CreateLocalProcessingRuleRequest.newBuilder()
                .setNewLocalProcessingRule(newLocalProcessingRule)
                .build())
        .getLocalProcessingRuleDetails();
  }
}
