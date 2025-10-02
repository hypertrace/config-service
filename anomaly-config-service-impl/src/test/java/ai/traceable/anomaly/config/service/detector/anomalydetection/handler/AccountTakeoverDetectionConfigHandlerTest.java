package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.detector.AccountTakeoverAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.CredentialStuffingAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccountTakeoverDetectionConfigHandlerTest {
  private AccountTakeoverDetectionConfigHandler accountTakeoverDetectionConfigHandler;

  @BeforeEach
  void setup() {
    accountTakeoverDetectionConfigHandler =
        new AccountTakeoverDetectionConfigHandler(mock(AccountTakeoverRulesRegistry.class));
  }

  @Test
  void test_merge_config() {
    ScopedAnomalyDetectionConfig preferredConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setAccountTakeoverAnomalyDetectionConfig(
                        AccountTakeoverAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("credentialStuffing")
                            .setCredentialStuffing(
                                CredentialStuffingAnomalyConfig.getDefaultInstance())
                            .build())
                    .build())
            .build();
    ScopedAnomalyDetectionConfig fallbackConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setAccountTakeoverAnomalyDetectionConfig(
                        AccountTakeoverAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("ato")
                            .setCredentialStuffing(
                                CredentialStuffingAnomalyConfig.getDefaultInstance())
                            .build())
                    .build())
            .build();
    assertEquals(
        List.of(
            AnomalyDetectionConfig.newBuilder()
                .setAccountTakeoverAnomalyDetectionConfig(
                    AccountTakeoverAnomalyDetectionConfig.newBuilder()
                        .setAnomalyRuleId("credentialStuffing")
                        .setCredentialStuffing(CredentialStuffingAnomalyConfig.getDefaultInstance())
                        .build())
                .build(),
            AnomalyDetectionConfig.newBuilder()
                .setAccountTakeoverAnomalyDetectionConfig(
                    AccountTakeoverAnomalyDetectionConfig.newBuilder()
                        .setAnomalyRuleId("ato")
                        .setCredentialStuffing(CredentialStuffingAnomalyConfig.getDefaultInstance())
                        .build())
                .build()),
        accountTakeoverDetectionConfigHandler.merge(preferredConfig, fallbackConfig));
  }

  @Test
  void merge_prefersPreferredAndPopulatesNewFields() {
    AccountTakeoverRulesRegistry registry = mock(AccountTakeoverRulesRegistry.class);
    AccountTakeoverAnomalyDetectionConfig defaultCfg =
        AccountTakeoverAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("ato")
            .setCredentialStuffing(CredentialStuffingAnomalyConfig.getDefaultInstance())
            .build();
    when(registry.getAccountTakeoverRuleIdToConfigMap())
        .thenReturn(Collections.singletonMap("ato", defaultCfg));

    AccountTakeoverDetectionConfigHandler handler =
        new AccountTakeoverDetectionConfigHandler(registry);

    ScopedAnomalyDetectionConfig preferred = getMonitorScopedWithAccountDef("ato_basic");
    ScopedAnomalyDetectionConfig fallback = getMonitorScopedWithAccountDef("ato_basic");

    List<AnomalyDetectionConfig> merged = handler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    AnomalyDetectionConfig out = merged.get(0);
    assertTrue(out.hasAccountTakeoverAnomalyDetectionConfig());
    assertEquals("ato", out.getAccountTakeoverAnomalyDetectionConfig().getAnomalyRuleId());

    AnomalySubRuleConfig sub =
        out.getAccountTakeoverAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("ato_basic");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, sub.getAnomalyRuleAction());

    preferred = getDisabledScopedWithAccountDef("ato_basic");
    fallback = getDisabledScopedWithAccountDef("ato_basic");

    merged = handler.merge(preferred, fallback);

    assertEquals(1, merged.size());
    out = merged.get(0);
    assertTrue(out.hasAccountTakeoverAnomalyDetectionConfig());
    assertEquals("ato", out.getAccountTakeoverAnomalyDetectionConfig().getAnomalyRuleId());

    sub =
        out.getAccountTakeoverAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("ato_basic");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, sub.getAnomalyRuleAction());
    assertTrue(sub.getInternal());

    preferred = getTestingScopedWithAccountDef("ato_basic");
    fallback = getTestingScopedWithAccountDef("ato_basic");
    merged = handler.merge(preferred, fallback);
    assertEquals(1, merged.size());
    out = merged.get(0);
    assertTrue(out.hasAccountTakeoverAnomalyDetectionConfig());
    assertEquals("ato", out.getAccountTakeoverAnomalyDetectionConfig().getAnomalyRuleId());
    sub =
        out.getAccountTakeoverAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsOrThrow("ato_basic");
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING, sub.getAnomalyRuleAction());
    assertFalse(sub.getInternal());
  }

  private ScopedAnomalyDetectionConfig getMonitorScopedWithAccountDef(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder monitoringConfig = AnomalySubRuleConfig.newBuilder();
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, monitoringConfig.setSubRuleId(id).build());
    }
    AccountTakeoverAnomalyDetectionConfig accountDef =
        AccountTakeoverAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("ato")
            .setCredentialStuffing(CredentialStuffingAnomalyConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setAccountTakeoverAnomalyDetectionConfig(accountDef)
                .build())
        .build();
  }

  private ScopedAnomalyDetectionConfig getDisabledScopedWithAccountDef(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder disabledConfig =
        AnomalySubRuleConfig.newBuilder()
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build());
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, disabledConfig.setSubRuleId(id).build());
    }
    AccountTakeoverAnomalyDetectionConfig accountDef =
        AccountTakeoverAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("ato")
            .setCredentialStuffing(CredentialStuffingAnomalyConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setAccountTakeoverAnomalyDetectionConfig(accountDef)
                .build())
        .build();
  }

  private ScopedAnomalyDetectionConfig getTestingScopedWithAccountDef(String... subRuleIds) {
    AnomalySubRuleConfigMap.Builder map = AnomalySubRuleConfigMap.newBuilder();
    AnomalySubRuleConfig.Builder disabledConfig =
        AnomalySubRuleConfig.newBuilder()
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_TESTING)
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build());
    for (String id : subRuleIds) {
      map.putSubRuleConfigs(id, disabledConfig.setSubRuleId(id).build());
    }
    AccountTakeoverAnomalyDetectionConfig accountDef =
        AccountTakeoverAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("ato")
            .setCredentialStuffing(CredentialStuffingAnomalyConfig.getDefaultInstance())
            .setSubRuleConfigs(map)
            .build();
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setAccountTakeoverAnomalyDetectionConfig(accountDef)
                .build())
        .build();
  }
}
