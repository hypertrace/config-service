package ai.traceable.anomaly.config.service.registry.session;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.ObjectBolaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UserIdBolaAnomalyConfig;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class SessionRulesRegistryTest {

  @Test
  public void testRules() {
    SessionRulesRegistryImpl sessionRulesRegistry =
        new SessionRulesRegistryImpl(new ConfigConverter());

    Map<String, AnomalyRuleInfo> anomalyRuleInfos = sessionRulesRegistry.getSessionRuleInfos();
    assertEquals(2, anomalyRuleInfos.size());
    assertEquals(
        "bola :: Authorization Bypass - Object Level\nuserIdBola :: Authorization Bypass - User Level",
        anomalyRuleInfos.values().stream()
            .map(
                anomalyRuleInfo ->
                    anomalyRuleInfo.getRuleId() + " :: " + anomalyRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));
  }

  @Test
  void testRuleIdToConfigMap() {
    SessionRulesRegistryImpl sessionDefinitionRegistry =
        new SessionRulesRegistryImpl(new ConfigConverter());

    Map<String, SessionDefinitionMetadataAnomalyDetectionConfig> ruleIdToConfigMap =
        sessionDefinitionRegistry.getSessionDefRuleIdToDetectionConfigMap();

    Map<String, SessionDefinitionMetadataAnomalyDetectionConfig> expectedMap =
        Map.of(
            "bola",
            SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("bola")
                .setObjectBola(ObjectBolaAnomalyConfig.getDefaultInstance())
                .build(),
            "userIdBola",
            SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("userIdBola")
                .setUserIdBola(UserIdBolaAnomalyConfig.getDefaultInstance())
                .build());

    assertEquals(expectedMap, ruleIdToConfigMap);

    Set<String> configRuleIds = ruleIdToConfigMap.keySet();
    Set<String> ruleInfoIds = sessionDefinitionRegistry.getSessionRuleInfos().keySet();

    assertEquals(ruleInfoIds, configRuleIds);
  }
}
