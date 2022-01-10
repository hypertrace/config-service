package ai.traceable.anomaly.config.service.detector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class DetectorConfigServiceConfigTest {

  @Test
  void testConfig() {
    DetectorConfigServiceConfig config =
        new DetectorConfigServiceConfig(
            ConfigFactory.parseString(
                "modsecDetectionConfigs =\n"
                    + "    [\n"
                    + "      {\n"
                    + "        configStatus = {\n"
                    + "          disabled = false\n"
                    + "          internal = false\n"
                    + "        }\n"
                    + "        modsecurityAnomalyDetectionConfig = {\n"
                    + "          anomalyRuleId = \"crs_912\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      {\n"
                    + "        configStatus = {\n"
                    + "          disabled = false\n"
                    + "          internal = false\n"
                    + "        }\n"
                    + "        modsecurityAnomalyDetectionConfig = {\n"
                    + "          anomalyRuleId = \"crs_913\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      {\n"
                    + "        configStatus = {\n"
                    + "          disabled = false\n"
                    + "          internal = false\n"
                    + "        }\n"
                    + "        modsecurityAnomalyDetectionConfig = {\n"
                    + "          anomalyRuleId = \"crs_921\"\n"
                    + "        }\n"
                    + "      }\n"
                    + "    ]"));

    List<AnomalyDetectionConfig> detectionConfigs = config.getDefaultModsecDetectionConfigs();
    List<String> ruleIds =
        detectionConfigs.stream()
            .map(
                detectionConfig ->
                    detectionConfig.getModsecurityAnomalyDetectionConfig().getAnomalyRuleId())
            .collect(Collectors.toList());
    assertEquals(List.of("crs_912", "crs_913", "crs_921"), ruleIds);
  }
}
