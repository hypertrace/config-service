package ai.traceable.anomaly.config.service.registry.apidef;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class ApiDefRulesRegistryTest {

  @Test
  public void testRules() {
    ApiDefRulesRegistryImpl apiDefRulesRegistry =
        new ApiDefRulesRegistryImpl(new ConfigConverter());

    Map<String, AnomalyRuleInfo> anomalyRuleInfos = apiDefRulesRegistry.getApiDefRuleInfos();
    assertEquals(10, anomalyRuleInfos.size());
    assertEquals(
        "contentSize :: Content Size Anomaly\n"
            + "contentType :: Content Type Anomaly\n"
            + "device :: Unexpected User Agent\n"
            + "enum :: Invalid Enumerations\n"
            + "httpStatus :: Unexpected HTTP Response Code\n"
            + "integer :: Value Out of Range\n"
            + "ssrf :: Server-Side Request Forgery\n"
            + "type :: Type Anomaly\n"
            + "unknownParam :: Unrecognized Field\n"
            + "xxe :: XML External Entity Injection",
        anomalyRuleInfos.values().stream()
            .map(
                anomalyRuleInfo ->
                    anomalyRuleInfo.getRuleId() + " :: " + anomalyRuleInfo.getRuleName())
            .sorted()
            .collect(Collectors.joining("\n")));
  }
}
