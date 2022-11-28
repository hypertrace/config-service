package ai.traceable.anomaly.config.service.registry.common;

import static ai.traceable.anomaly.config.service.v1.AnomalyRuleCategory.ANOMALY_RULE_CATEGORY_AUTHORIZATION_VIOLATIONS;
import static ai.traceable.anomaly.config.service.v1.AnomalyRuleCategory.ANOMALY_RULE_CATEGORY_PARAMETER_ANOMALIES;
import static ai.traceable.anomaly.config.service.v1.AnomalyRuleCategory.ANOMALY_RULE_CATEGORY_SCANNER_DETECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigConverterTest {
  @Test
  void testConvertAnomalyRuleInfos() {
    ConfigConverter configConverter = new ConfigConverter();
    List<? extends Config> configsList =
        ConfigFactory.parseString(
                "rules = [\n"
                    + "  {\n"
                    + "    ruleId = \"crs_913\"\n"
                    + "    ruleName = \"Scanner Detection\"\n"
                    + "    anomalyRuleCategory = ANOMALY_RULE_CATEGORY_SCANNER_DETECTED\n"
                    + "  },\n"
                    + "  {\n"
                    + "    ruleId = \"enum\"\n"
                    + "    ruleName = \"Invalid Enumerations\"\n"
                    + "    anomalyRuleCategory = ANOMALY_RULE_CATEGORY_PARAMETER_ANOMALIES\n"
                    + "  },\n"
                    + "  {\n"
                    + "    ruleId = \"bola\"\n"
                    + "    ruleName = \"Authorization Bypass\"\n"
                    + "    anomalyRuleCategory = ANOMALY_RULE_CATEGORY_AUTHORIZATION_VIOLATIONS\n"
                    + "  }\n"
                    + "]")
            .getConfigList("rules");

    Map<String, AnomalyRuleInfo> ruleInfosMap =
        configConverter.convertAnomalyRuleInfos(
            configsList, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF);
    assertEquals(3, ruleInfosMap.size());

    assertEquals("crs_913", ruleInfosMap.get("crs_913").getRuleId());
    assertEquals("Scanner Detection", ruleInfosMap.get("crs_913").getRuleName());
    assertEquals("Scanner Detection", ruleInfosMap.get("crs_913").getRuleCategory());
    assertEquals(
        ANOMALY_RULE_CATEGORY_SCANNER_DETECTED,
        ruleInfosMap.get("crs_913").getAnomalyRuleCategory());
    assertEquals(
        AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF,
        ruleInfosMap.get("crs_913").getEventFamily());

    assertEquals("Parameter Anomalies", ruleInfosMap.get("enum").getRuleCategory());
    assertEquals("enum", ruleInfosMap.get("enum").getRuleId());
    assertEquals("Invalid Enumerations", ruleInfosMap.get("enum").getRuleName());
    assertEquals(
        ANOMALY_RULE_CATEGORY_PARAMETER_ANOMALIES,
        ruleInfosMap.get("enum").getAnomalyRuleCategory());
    assertEquals(
        AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF, ruleInfosMap.get("enum").getEventFamily());

    assertEquals("bola", ruleInfosMap.get("bola").getRuleId());
    assertEquals("Authorization Bypass", ruleInfosMap.get("bola").getRuleName());
    assertEquals("Authorization Violations", ruleInfosMap.get("bola").getRuleCategory());
    assertEquals(
        ANOMALY_RULE_CATEGORY_AUTHORIZATION_VIOLATIONS,
        ruleInfosMap.get("bola").getAnomalyRuleCategory());
    assertEquals(
        AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF, ruleInfosMap.get("bola").getEventFamily());
  }

  @Test
  void testErrorOnInvalidRuleCategory() {
    ConfigConverter configConverter = new ConfigConverter();
    List<? extends Config> configsList =
        ConfigFactory.parseString(
                "rules = [\n"
                    + "  {\n"
                    + "    ruleId = \"crs_913\"\n"
                    + "    ruleName = \"Scanner Detection\"\n"
                    + "    anomalyRuleCategory = ANOMALY_RULE_CATEGORY_ILLEGAL\n"
                    + "  }\n"
                    + "]")
            .getConfigList("rules");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            configConverter.convertAnomalyRuleInfos(
                configsList, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC));
  }
}
