package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AccountTakeoverAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CredentialStuffingAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
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
}
