package ai.traceable.anomaly.config.service.detector;

import static ai.traceable.anomaly.config.service.v1.detector.AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH;
import static ai.traceable.anomaly.config.service.v1.detector.AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.detector.AbuseVelocity;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.CodeDetectedInPromptThreatRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.EmailDomainAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.IpTypeAnomalyConfig;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
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
                  + "apiProtectDetectionConfigs = []\n"
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
                  + "genAiDetectionConfigs = [\n"
                  + "    {\n"
                  + "      genAiAnomalyDetectionConfig = {\n"
                  + "        anomalyRuleId = \"codeDetectedInPrompt\"\n"
                  + "        codeDetectedInPrompt = {\n"
                  + "          threatRuleConfigs = [\n"
                  + "            {\n"
                  + "              threatRuleId = \"crs_941\"\n"
                  + "              subRuleIds = {\n"
                  + "                values = [\"crs_9410170\", \"crs_9410160\", \"crs_941110\"]\n"
                  + "              }\n"
                  + "            }\n"
                  + "            {\n"
                  + "              threatRuleId = \"crs_942\"\n"
                  + "              subRuleIds = {\n"
                  + "                values = [\"crs_9420190\", \"crs_9420291\"]\n"
                  + "              }\n"
                  + "            }\n"
                  + "          ]"
                  + "        }\n"
                  + "        subRuleConfigs = {\n"
                  + "          subRuleConfigs = {\n"
                  + "            crs_941 = {\n"
                  + "              subRuleId = \"crs_932\"\n"
                  + "              categoryConfig = {\n"
                  + "                eventScoreCategory = ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM\n"
                  + "              }\n"
                  + "            }\n"
                  + "            crs_942 = {\n"
                  + "              subRuleId = \"crs_932\"\n"
                  + "              categoryConfig = {\n"
                  + "                eventScoreCategory = ANOMALY_EVENT_SCORE_CATEGORY_HIGH\n"
                  + "              }\n"
                  + "            }\n"
                  + "          }\n"
                  + "        }"
                  + "      }\n"
                  + "    }\n"
                  + "  ]\n"
                  + "credentialStuffingDetectionConfigs = [\n"
                  + " {\n"
                  + "   configStatus = {\n"
                  + "        disabled = true\n"
                  + "        internal = true\n"
                  + "      }\n"
                  + "      credentialAnomalyDetectionConfig = {\n"
                  + "        anomalyRuleId = \"credentialStuffing\"\n"
                  + "      }\n"
                  + "    }\n"
                  + "]\n"
                  + "accountTakeoverDetectionConfigs = [\n"
                  + " {\n"
                  + "   configStatus = {\n"
                  + "        disabled = true\n"
                  + "        internal = true\n"
                  + "      }\n"
                  + "      accountTakeoverAnomalyDetectionConfig = {\n"
                  + "        anomalyRuleId = \"ato\"\n"
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
          new VolumetricRulesRegistryImpl(new ConfigConverter()),
          new CredentialStuffingRulesRegistryImpl(new ConfigConverter()),
          new AccountTakeoverRulesRegistryImpl(new ConfigConverter()),
          new GenAiRulesRegistryImpl(new ConfigConverter()));

  @Test
  void testConfig() {
    AnomalyConfigStatusChange configStatus0 = AnomalyConfigStatusChange.newBuilder().build();
    AnomalyConfigStatusChange configStatus1 =
        AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(false).build();
    AnomalyConfigStatusChange configStatus2 =
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(false).build();
    AnomalyConfigStatusChange configStatus3 =
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(true).build();

    List<AnomalyDetectionConfig> modsecConfigs = CONFIG.getDefaultWafDetectionConfigs();
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

    List<AnomalyDetectionConfig> apiProtectionDetectionConfigs =
        CONFIG.getDefaultApiProtectionDetectionConfigs();

    detectionConfig = getApiDefDetectionConfig(apiProtectionDetectionConfigs, "integer");
    assertEquals(configStatus0, detectionConfig.getConfigStatus());

    detectionConfig = getApiDefDetectionConfig(apiProtectionDetectionConfigs, "missingParam");
    assertEquals(configStatus2, detectionConfig.getConfigStatus());

    detectionConfig = getApiDefDetectionConfig(apiProtectionDetectionConfigs, "enum");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());

    detectionConfig = getSessionDefDetectionConfig(apiProtectionDetectionConfigs, "bola");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());

    detectionConfig = getSessionDefDetectionConfig(apiProtectionDetectionConfigs, "userIdBola");
    assertEquals(configStatus0, detectionConfig.getConfigStatus());

    detectionConfig =
        getVolumetricDetectionConfig(apiProtectionDetectionConfigs, "volumetricApiCallSpike");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());

    detectionConfig =
        getCredentialStuffingDetectionConfig(apiProtectionDetectionConfigs, "credentialStuffing");
    assertEquals(configStatus3, detectionConfig.getConfigStatus());

    List<AnomalyDetectionConfig> genAiDetectionConfigs = CONFIG.getDefaultGenAiDetectionConfigs();
    detectionConfig = getGenAiDetectionConfig(genAiDetectionConfigs, "codeDetectedInPrompt");
    List<CodeDetectedInPromptThreatRuleConfig> threatRuleConfigs =
        detectionConfig
            .getGenAiAnomalyDetectionConfig()
            .getCodeDetectedInPrompt()
            .getThreatRuleConfigsList();
    Map<String, AnomalySubRuleConfig> subRuleConfigMap =
        detectionConfig.getGenAiAnomalyDetectionConfig().getSubRuleConfigs().getSubRuleConfigsMap();
    assertEquals(2, threatRuleConfigs.size());
    assertEquals("crs_941", threatRuleConfigs.get(0).getThreatRuleId());
    assertEquals(3, threatRuleConfigs.get(0).getSubRuleIds().getValuesCount());
    assertEquals("crs_942", threatRuleConfigs.get(1).getThreatRuleId());
    assertEquals(2, threatRuleConfigs.get(1).getSubRuleIds().getValuesCount());
    assertEquals(
        ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM,
        subRuleConfigMap.get("crs_941").getCategoryConfig().getEventScoreCategory());
    assertEquals(
        ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
        subRuleConfigMap.get("crs_942").getCategoryConfig().getEventScoreCategory());
  }

  @Test
  void testCustomRulesDetectionConfigs() {
    List<AnomalyDetectionConfig> customRulesDetectionConfigs =
        CONFIG.getDefaultApiProtectionDetectionConfigs().stream()
            .filter(config -> config.hasCustomRulesAnomalyDetectionConfig())
            .collect(Collectors.toUnmodifiableList());

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
        ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
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
        ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
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

  private AnomalyDetectionConfig getCredentialStuffingDetectionConfig(
      List<AnomalyDetectionConfig> detectionConfigs, String ruleId) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig.getCredentialAnomalyDetectionConfig().getAnomalyRuleId().equals(ruleId)) {
        return detectionConfig;
      }
    }
    return null;
  }

  private AnomalyDetectionConfig getGenAiDetectionConfig(
      List<AnomalyDetectionConfig> detectionConfigs, String ruleId) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig.getGenAiAnomalyDetectionConfig().getAnomalyRuleId().equals(ruleId)) {
        return detectionConfig;
      }
    }
    return null;
  }
}
