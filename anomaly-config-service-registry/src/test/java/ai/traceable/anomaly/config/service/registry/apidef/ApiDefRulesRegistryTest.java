package ai.traceable.anomaly.config.service.registry.apidef;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ApiDefRulesRegistryTest {

  @Test
  public void testRules() {
    ApiDefRulesRegistryImpl apiDefRulesRegistry =
        new ApiDefRulesRegistryImpl(new ConfigConverter());

    Map<String, AnomalyRuleInfo> anomalyRuleInfos = apiDefRulesRegistry.getApiDefRuleInfos();
    assertEquals(10, anomalyRuleInfos.size());
  }
}
