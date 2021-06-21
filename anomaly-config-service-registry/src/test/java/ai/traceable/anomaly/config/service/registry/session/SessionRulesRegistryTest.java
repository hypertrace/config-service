package ai.traceable.anomaly.config.service.registry.session;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class SessionRulesRegistryTest {

  @Test
  public void testRules() {
    SessionRulesRegistryImpl sessionRulesRegistry =
        new SessionRulesRegistryImpl(new ConfigConverter());

    Map<String, AnomalyRuleInfo> anomalyRuleInfos = sessionRulesRegistry.getSessionRuleInfos();
    assertEquals(1, anomalyRuleInfos.size());
    assertEquals(
        "bola :: Authorization Bypass",
        anomalyRuleInfos.values().stream()
            .map(
                anomalyRuleInfo ->
                    anomalyRuleInfo.getRuleId() + " :: " + anomalyRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));
  }
}
