package ai.traceable.anomaly.config.service.registry.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ConfigConverterTest {

  @Test
  public void testConvertAnomalyRuleInfos() {

    ConfigConverter configConverter = new ConfigConverter();
    List<? extends Config> configsList =
        ConfigFactory.parseString(
                "rules = [\n"
                    + "  {\n"
                    + "    ruleId = \"crs_913\"\n"
                    + "    ruleName = \"Scanner Detection\"\n"
                    + "  },\n"
                    + "  {\n"
                    + "    ruleId = \"enum\"\n"
                    + "    ruleName = \"Invalid Enumerations\"\n"
                    + "    ruleCategory = \"Parameter Anomalies\"\n"
                    + "  },\n"
                    + "  {\n"
                    + "    ruleId = \"xyz\"\n"
                    + "    ruleName = \"XYZ Something\"\n"
                    + "  }\n"
                    + "]")
            .getConfigList("rules");

    Map<String, AnomalyRuleInfo> ruleInfosMap =
        configConverter.convertAnomalyRuleInfos(
            configsList, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF);
    assertEquals(3, ruleInfosMap.size());

    assertTrue(ruleInfosMap.get("crs_913").getRuleCategory().isEmpty());
    assertEquals("crs_913", ruleInfosMap.get("crs_913").getRuleId());
    assertEquals("Scanner Detection", ruleInfosMap.get("crs_913").getRuleName());
    assertEquals(
        AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF,
        ruleInfosMap.get("crs_913").getEventFamily());

    assertEquals("Parameter Anomalies", ruleInfosMap.get("enum").getRuleCategory());
    assertEquals("enum", ruleInfosMap.get("enum").getRuleId());
    assertEquals("Invalid Enumerations", ruleInfosMap.get("enum").getRuleName());
    assertEquals(
        AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF, ruleInfosMap.get("enum").getEventFamily());

    assertTrue(ruleInfosMap.get("xyz").getRuleCategory().isEmpty());
    assertEquals("xyz", ruleInfosMap.get("xyz").getRuleId());
    assertEquals("XYZ Something", ruleInfosMap.get("xyz").getRuleName());
    assertEquals(
        AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF, ruleInfosMap.get("xyz").getEventFamily());
  }
}
