package ai.traceable.localprocessing.config.service.ruleservice;

import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.DEFAULT_PROTECTION_MODE;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.MODSEC_REDACT_MESSAGES;
import static ai.traceable.localprocessing.config.service.constants.LocalProcessingConstants.SAMPLING_POLICIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import ai.traceable.localprocessing.config.service.LocalProcessingConfigServiceConfig;
import ai.traceable.localprocessing.config.service.coordinator.ConfigServiceCoordinatorImpl;
import ai.traceable.localprocessing.config.service.coordinator.DefaultProtectionModeConfigStore;
import ai.traceable.localprocessing.config.service.coordinator.LocalProcessingRulesConfigStore;
import ai.traceable.localprocessing.config.service.v1.CreateLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.DeleteLocalProcessingRuleRequest;
import ai.traceable.localprocessing.config.service.v1.GetAllLocalProcessingRulesRequest;
import ai.traceable.localprocessing.config.service.v1.GetDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleDetails;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRuleMetadata;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc;
import ai.traceable.localprocessing.config.service.v1.LocalProcessingRulesServiceGrpc.LocalProcessingRulesServiceBlockingStub;
import ai.traceable.localprocessing.config.service.v1.NewLocalProcessingRule;
import ai.traceable.localprocessing.config.service.v1.ProtectionMode;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeRequest;
import ai.traceable.localprocessing.config.service.v1.UpdateDefaultProtectionModeResponse;
import ai.traceable.localprocessing.config.service.v1.UpdateLocalProcessingRuleRequest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalProcessingRulesServiceImplTest {

  LocalProcessingRulesServiceBlockingStub localProcessingRulesStub;
  MockGenericConfigService mockGenericConfigService;

  @BeforeEach
  void beforeEach() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    Config config =
        ConfigFactory.parseMap(
            Map.of(
                LOCAL_PROCESSING_CONFIG_SERVICE_CONFIG,
                Map.of(
                    DEFAULT_PROTECTION_MODE,
                    ProtectionMode.PROTECTION_MODE_ADVANCED.name(),
                    SAMPLING_POLICIES,
                    List.of(),
                    MODSEC_REDACT_MESSAGES,
                    true)));
    mockGenericConfigService
        .addService(
            new LocalProcessingRulesServiceImpl(
                new ConfigServiceCoordinatorImpl(
                    new LocalProcessingConfigServiceConfig(config),
                    new DefaultProtectionModeConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new LocalProcessingRulesConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator))))
        .start();

    localProcessingRulesStub =
        LocalProcessingRulesServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void createReadUpdateDeleteLocalProcessingRules() {
    NewLocalProcessingRule newLocalProcessingRule1 =
        NewLocalProcessingRule.newBuilder()
            .setUrlPattern("/checkout/*")
            .setHostHeader("abc.com")
            .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
            .build();
    NewLocalProcessingRule newLocalProcessingRule2 =
        NewLocalProcessingRule.newBuilder()
            .setUrlPattern("/orders/**")
            .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
            .build();
    LocalProcessingRuleDetails localProcessingRuleDetails1 =
        localProcessingRulesStub
            .createLocalProcessingRule(
                CreateLocalProcessingRuleRequest.newBuilder()
                    .setNewLocalProcessingRule(newLocalProcessingRule1)
                    .build())
            .getLocalProcessingRuleDetails();
    assertEquals(
        buildLocalProcessingRuleDetails(
            newLocalProcessingRule1,
            localProcessingRuleDetails1.getRule().getId(),
            localProcessingRuleDetails1.getMetadata().getCreationTimestamp()),
        localProcessingRuleDetails1);

    LocalProcessingRuleDetails localProcessingRuleDetails2 =
        localProcessingRulesStub
            .createLocalProcessingRule(
                CreateLocalProcessingRuleRequest.newBuilder()
                    .setNewLocalProcessingRule(newLocalProcessingRule2)
                    .build())
            .getLocalProcessingRuleDetails();

    assertEquals(
        List.of(localProcessingRuleDetails2, localProcessingRuleDetails1),
        localProcessingRulesStub
            .getAllLocalProcessingRules(GetAllLocalProcessingRulesRequest.getDefaultInstance())
            .getLocalProcessingRulesDetailsList());

    LocalProcessingRule ruleToUpdate =
        LocalProcessingRule.newBuilder()
            .setId(localProcessingRuleDetails1.getRule().getId())
            .setUrlPattern("/checkout/v1/*")
            .setHostHeader("abc.com")
            .setProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
            .build();
    LocalProcessingRuleDetails updatedRuleDetails =
        localProcessingRulesStub
            .updateLocalProcessingRule(
                UpdateLocalProcessingRuleRequest.newBuilder()
                    .setLocalProcessingRule(ruleToUpdate)
                    .build())
            .getLocalProcessingRuleDetails();
    assertEquals(
        buildLocalProcessingRuleDetails(
            ruleToUpdate, localProcessingRuleDetails1.getMetadata().getCreationTimestamp()),
        updatedRuleDetails);

    assertEquals(
        List.of(localProcessingRuleDetails2, updatedRuleDetails),
        localProcessingRulesStub
            .getAllLocalProcessingRules(GetAllLocalProcessingRulesRequest.getDefaultInstance())
            .getLocalProcessingRulesDetailsList());

    localProcessingRulesStub.deleteLocalProcessingRule(
        DeleteLocalProcessingRuleRequest.newBuilder()
            .setLocalProcessingRuleId(localProcessingRuleDetails2.getRule().getId())
            .build());
    assertEquals(
        List.of(updatedRuleDetails),
        localProcessingRulesStub
            .getAllLocalProcessingRules(GetAllLocalProcessingRulesRequest.getDefaultInstance())
            .getLocalProcessingRulesDetailsList());
  }

  @Test
  void upsertAndGetDefaultProtectionMode() {
    ProtectionMode defaultProtectionMode =
        localProcessingRulesStub
            .getDefaultProtectionMode(GetDefaultProtectionModeRequest.newBuilder().build())
            .getDefaultProtectionMode();
    assertEquals(ProtectionMode.PROTECTION_MODE_ADVANCED, defaultProtectionMode);

    UpdateDefaultProtectionModeResponse response =
        localProcessingRulesStub.updateDefaultProtectionMode(
            UpdateDefaultProtectionModeRequest.newBuilder()
                .setDefaultProtectionMode(ProtectionMode.PROTECTION_MODE_CORE)
                .build());
    assertEquals(ProtectionMode.PROTECTION_MODE_CORE, response.getDefaultProtectionMode());

    defaultProtectionMode =
        localProcessingRulesStub
            .getDefaultProtectionMode(GetDefaultProtectionModeRequest.newBuilder().build())
            .getDefaultProtectionMode();
    assertEquals(ProtectionMode.PROTECTION_MODE_CORE, defaultProtectionMode);
  }

  private LocalProcessingRuleDetails buildLocalProcessingRuleDetails(
      NewLocalProcessingRule newLocalProcessingRule, String id, long creationTimestamp) {
    LocalProcessingRule localProcessingRule =
        LocalProcessingRule.newBuilder()
            .setId(id)
            .setUrlPattern(newLocalProcessingRule.getUrlPattern())
            .setHostHeader(newLocalProcessingRule.getHostHeader())
            .setProtectionMode(newLocalProcessingRule.getProtectionMode())
            .build();
    return buildLocalProcessingRuleDetails(localProcessingRule, creationTimestamp);
  }

  private LocalProcessingRuleDetails buildLocalProcessingRuleDetails(
      LocalProcessingRule localProcessingRule, long creationTimestamp) {
    return LocalProcessingRuleDetails.newBuilder()
        .setRule(localProcessingRule)
        .setMetadata(
            LocalProcessingRuleMetadata.newBuilder()
                .setCreationTimestamp(creationTimestamp)
                .build())
        .build();
  }
}
