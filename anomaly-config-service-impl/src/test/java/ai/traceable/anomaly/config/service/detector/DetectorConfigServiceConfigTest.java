package ai.traceable.anomaly.config.service.detector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.detector.AbuseVelocity;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyEventScoreCategory;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.EmailDomainAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.IpTypeAnomalyConfig;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class DetectorConfigServiceConfigTest {

  private static final DetectorConfigServiceConfig CONFIG =
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
                  + "           modsecAnomalyRule = {\n"
                  + "              anomalyRuleId = \"crs_912\"\n"
                  + "              subRuleConfigs = [\n"
                  + "              {\n"
                  + "                subRuleId = \"subRule1\"\n"
                  + "                configStatus = {\n"
                  + "                  disabled = true\n"
                  + "                  internal = true\n"
                  + "                }\n"
                  + "              }\n"
                  + "            ]\n"
                  + "           }\n"
                  + "        }\n"
                  + "      },\n"
                  + "      {\n"
                  + "        configStatus = {\n"
                  + "          disabled = true\n"
                  + "          internal = false\n"
                  + "        }\n"
                  + "        modsecurityAnomalyDetectionConfig = {\n"
                  + "           modsecAnomalyRule = {\n"
                  + "              anomalyRuleId = \"crs_913\"\n"
                  + "           }\n"
                  + "        }\n"
                  + "      },\n"
                  + "      {\n"
                  + "        configStatus = {\n"
                  + "          disabled = true\n"
                  + "          internal = true\n"
                  + "        }\n"
                  + "        modsecurityAnomalyDetectionConfig = {\n"
                  + "           modsecAnomalyRule = {\n"
                  + "              anomalyRuleId = \"crs_921\"\n"
                  + "           }\n"
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
                  + " ]\n"
                  + "sessionDefinitionDetectionConfigs = [\n"
                  + " {\n"
                  + "   configStatus = {\n"
                  + "        disabled = true\n"
                  + "        internal = true\n"
                  + "      }\n"
                  + "      sessionDefinitionMetadataAnomalyDetectionConfig = {\n"
                  + "        anomalyRuleId = \"bola\"\n"
                  + "      }\n"
                  + "    }\n"
                  + "]\n"
                  + "volumetricDetectionConfigs = [\n"
                  + " {\n"
                  + "   configStatus = {\n"
                  + "        disabled = true\n"
                  + "        internal = true\n"
                  + "      }\n"
                  + "      volumetricAnomalyDetectionConfig = {\n"
                  + "        anomalyRuleId = \"volumetricApiCallSpike\"\n"
                  + "      }\n"
                  + "    }\n"
                  + "]\n"
                  + "customRulesDetectionConfigs = [\n"
                  + "    {\n"
                  + "      categoryConfig = {\n"
                  + "        eventScoreCategory = \"ANOMALY_EVENT_SCORE_CATEGORY_HIGH\"\n"
                  + "      }\n"
                  + "      customRulesAnomalyDetectionConfig = {\n"
                  + "        maliciousSources = {\n"
                  + "          emailDomain = {\n"
                  + "            highEmailFraudScoreMinThreshold = 85\n"
                  + "            criticalEmailFraudScoreMinThreshold = 95\n"
                  + "            disabled = true\n"
                  + "          }\n"
                  + "        }\n"
                  + "      }\n"
                  + "    },\n"
                  + "    {\n"
                  + "      categoryConfig = {\n"
                  + "        eventScoreCategory = \"ANOMALY_EVENT_SCORE_CATEGORY_HIGH\"\n"
                  + "      }\n"
                  + "      customRulesAnomalyDetectionConfig = {\n"
                  + "        maliciousSources = {\n"
                  + "          ipType = {\n"
                  + "            abuseVelocityMinThreshold = \"ABUSE_VELOCITY_HIGH\"\n"
                  + "            ipReputationScoreMinThreshold = 100\n"
                  + "          }\n"
                  + "        }\n"
                  + "      }\n"
                  + "    }\n"
                  + "  ]"),
          new ApiDefinitionRegistryImpl(new ConfigConverter()),
          new SessionRulesRegistryImpl(new ConfigConverter()),
          new VolumetricRulesRegistryImpl(new ConfigConverter()));

  @Test
  void testConfig() {
    AnomalyConfigStatusChange configStatus0 = AnomalyConfigStatusChange.newBuilder().build();
    AnomalyConfigStatusChange configStatus1 =
        AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(false).build();
    AnomalyConfigStatusChange configStatus2 =
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(false).build();
    AnomalyConfigStatusChange configStatus3 =
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build();

    List<AnomalyDetectionConfig> modsecConfigs = CONFIG.getDefaultModsecDetectionConfigs();
    List<String> ruleIds =
        modsecConfigs.stream()
            .map(
                detectionConfig ->
                    detectionConfig
                        .getModsecurityAnomalyDetectionConfig()
                        .getModsecAnomalyRule()
                        .getAnomalyRuleId())
            .collect(Collectors.toList());
    assertEquals(List.of("crs_912", "crs_913", "crs_921"), ruleIds);

    AnomalyDetectionConfig detectionConfig = getModsecConfig(modsecConfigs, "crs_912");
    assertEquals(configStatus1, detectionConfig.getConfigStatus());

    assertEquals(
        configStatus3,
        detectionConfig
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getConfigStatus());

    detectionConfig = getModsecConfig(modsecConfigs, "crs_913");
    assertEquals(configStatus2, detectionConfig.getConfigStatus());

    detectionConfig = getModsecConfig(modsecConfigs, "crs_921");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());

    List<AnomalyDetectionConfig> apiDefinitionDetectionConfigs =
        CONFIG.getDefaultApiDefinitionDetectionConfigs();

    detectionConfig = getApiDefDetectionConfig(apiDefinitionDetectionConfigs, "integer");
    assertEquals(configStatus0, detectionConfig.getConfigStatus());

    detectionConfig = getApiDefDetectionConfig(apiDefinitionDetectionConfigs, "missingParam");
    assertEquals(configStatus2, detectionConfig.getConfigStatus());

    detectionConfig = getApiDefDetectionConfig(apiDefinitionDetectionConfigs, "enum");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());

    List<AnomalyDetectionConfig> sessionDefinitionDetectionConfigs =
        CONFIG.getDefaultSessionDefinitionDetectionConfigs();

    detectionConfig = getSessionDefDetectionConfig(sessionDefinitionDetectionConfigs, "bola");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());

    detectionConfig = getSessionDefDetectionConfig(sessionDefinitionDetectionConfigs, "userIdBola");
    assertEquals(configStatus0, detectionConfig.getConfigStatus());

    List<AnomalyDetectionConfig> volumetricDetectionConfigs =
        CONFIG.getDefaultVolumetricDetectionConfigs();

    detectionConfig =
        getVolumetricDetectionConfig(volumetricDetectionConfigs, "volumetricApiCallSpike");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());
  }

  @Test
  void testCustomRulesDetectionConfigs() {
    List<AnomalyDetectionConfig> customRulesDetectionConfigs =
        CONFIG.getDefaultCustomRulesDetectionConfigs();

    AnomalyDetectionConfig detectionConfig =
        customRulesDetectionConfigs.stream()
            .filter(
                config ->
                    config
                        .getCustomRulesAnomalyDetectionConfig()
                        .getMaliciousSources()
                        .hasEmailDomain())
            .findFirst()
            .orElse(null);
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
        detectionConfig.getCategoryConfig().getEventScoreCategory());

    EmailDomainAnomalyConfig emailDomainConfig =
        detectionConfig
            .getCustomRulesAnomalyDetectionConfig()
            .getMaliciousSources()
            .getEmailDomain();
    assertEquals(85, emailDomainConfig.getHighEmailFraudScoreMinThreshold());
    assertEquals(95, emailDomainConfig.getCriticalEmailFraudScoreMinThreshold());
    assertTrue(emailDomainConfig.getDisabled());

    detectionConfig =
        customRulesDetectionConfigs.stream()
            .filter(
                config ->
                    config.getCustomRulesAnomalyDetectionConfig().getMaliciousSources().hasIpType())
            .findFirst()
            .orElse(null);
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
        detectionConfig.getCategoryConfig().getEventScoreCategory());

    IpTypeAnomalyConfig ipTypeConfig =
        detectionConfig.getCustomRulesAnomalyDetectionConfig().getMaliciousSources().getIpType();
    assertEquals(AbuseVelocity.ABUSE_VELOCITY_HIGH, ipTypeConfig.getAbuseVelocityMinThreshold());
    assertEquals(100, ipTypeConfig.getIpReputationScoreMinThreshold());
  }

  private AnomalyDetectionConfig getModsecConfig(
      List<AnomalyDetectionConfig> detectionConfigs, String ruleId) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig
          .getModsecurityAnomalyDetectionConfig()
          .getModsecAnomalyRule()
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

  private AnomalyDetectionConfig getSessionDefDetectionConfig(
      List<AnomalyDetectionConfig> detectionConfigs, String ruleId) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig
          .getSessionDefinitionMetadataAnomalyDetectionConfig()
          .getAnomalyRuleId()
          .equals(ruleId)) {
        return detectionConfig;
      }
    }
    return null;
  }

  private AnomalyDetectionConfig getVolumetricDetectionConfig(
      List<AnomalyDetectionConfig> detectionConfigs, String ruleId) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId().equals(ruleId)) {
        return detectionConfig;
      }
    }
    return null;
  }
}
