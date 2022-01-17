package ai.traceable.anomaly.config.service.detector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
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
                    + "          disabled = true\n"
                    + "          internal = false\n"
                    + "        }\n"
                    + "        modsecurityAnomalyDetectionConfig = {\n"
                    + "          anomalyRuleId = \"crs_913\"\n"
                    + "        }\n"
                    + "      },\n"
                    + "      {\n"
                    + "        configStatus = {\n"
                    + "          disabled = true\n"
                    + "          internal = true\n"
                    + "        }\n"
                    + "        modsecurityAnomalyDetectionConfig = {\n"
                    + "          anomalyRuleId = \"crs_921\"\n"
                    + "        }\n"
                    + "      }\n"
                    + "    ]\n"
                    + "apiDefinitionDetectionConfigs = [\n"
                    + "    {\n"
                    + "      configStatus = {\n"
                    + "        disabled = true\n"
                    + "        internal = false\n"
                    + "      }\n"
                    + "      apiDefinitionMetadataAnomalyDetectionConfig = {\n"
                    + "        anomalyRuleId = \"missingParam\"\n"
                    + "      }\n"
                    + "    },\n"
                    + "    {\n"
                    + "      configStatus = {\n"
                    + "        disabled = true\n"
                    + "        internal = true\n"
                    + "      }\n"
                    + "      apiDefinitionMetadataAnomalyDetectionConfig = {\n"
                    + "        anomalyRuleId = \"enum\"\n"
                    + "      }\n"
                    + "    }\n"
                    + " ]"),
            new ApiDefinitionRegistryImpl(new ConfigConverter()));

    AnomalyConfigStatus configStatus1 =
        AnomalyConfigStatus.newBuilder().setDisabled(false).setInternal(false).build();
    AnomalyConfigStatus configStatus2 =
        AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(false).build();
    AnomalyConfigStatus configStatus3 =
        AnomalyConfigStatus.newBuilder().setDisabled(true).setInternal(true).build();

    List<AnomalyDetectionConfig> modsecConfigs = config.getDefaultModsecDetectionConfigs();
    List<String> ruleIds =
        modsecConfigs.stream()
            .map(
                detectionConfig ->
                    detectionConfig.getModsecurityAnomalyDetectionConfig().getAnomalyRuleId())
            .collect(Collectors.toList());
    assertEquals(List.of("crs_912", "crs_913", "crs_921"), ruleIds);

    AnomalyDetectionConfig detectionConfig = getModsecConfig(modsecConfigs, "crs_912");
    assertEquals(configStatus1, detectionConfig.getConfigStatus());

    detectionConfig = getModsecConfig(modsecConfigs, "crs_913");
    assertEquals(configStatus2, detectionConfig.getConfigStatus());

    detectionConfig = getModsecConfig(modsecConfigs, "crs_921");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());

    List<AnomalyDetectionConfig> apiDefinitionDetectionConfigs =
        config.getDefaultApiDefinitionDetectionConfigs();

    detectionConfig = getApiDefDetectionConfig(apiDefinitionDetectionConfigs, "integer");
    assertEquals(configStatus1, detectionConfig.getConfigStatus());

    detectionConfig = getApiDefDetectionConfig(apiDefinitionDetectionConfigs, "missingParam");
    assertEquals(configStatus2, detectionConfig.getConfigStatus());

    detectionConfig = getApiDefDetectionConfig(apiDefinitionDetectionConfigs, "enum");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());
  }

  private AnomalyDetectionConfig getModsecConfig(
      List<AnomalyDetectionConfig> detectionConfigs, String ruleId) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig
          .getModsecurityAnomalyDetectionConfig()
          .getAnomalyRuleId()
          .equals(ruleId)) {
        return detectionConfig;
      }
    }
    return null;
  }

  private AnomalyDetectionConfig getApiDefDetectionConfig(
      List<AnomalyDetectionConfig> detectionConfigs, String ruleId) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig
          .getApiDefinitionMetadataAnomalyDetectionConfig()
          .getAnomalyRuleId()
          .equals(ruleId)) {
        return detectionConfig;
      }
    }
    return null;
  }
}
