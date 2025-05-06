package ai.traceable.anomaly.config.service.trainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.AnomalyConfigServiceConfig;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.trainer.AccessorsTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ContentSizeTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.EnumerationsTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdCountConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdFamilyConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ThresholdsFamilyTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UserAttributionTrainingConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TrainerConfigServiceConfigTest {
  private static final String FILE_PATH = "trainer/trainer-config-service-config.conf";

  @Test
  void testConfig() {
    Config trainerServiceConfig = ConfigFactory.parseResources(FILE_PATH);
    AnomalyConfigServiceConfig anomalyConfigServiceConfig =
        new AnomalyConfigServiceConfig(trainerServiceConfig);
    ConfigConverter configConverter = new ConfigConverter();
    TrainerConfigServiceConfig trainerConfigServiceConfig =
        new TrainerConfigServiceConfig(anomalyConfigServiceConfig, configConverter);
    List<TrainingConfig> apiNamingTrainingConfigs =
        trainerConfigServiceConfig.getApiNamingTrainingConfigs();
    List<TrainingConfig> metadataTrainingConfigs =
        trainerConfigServiceConfig.getMetadataTrainingConfigs();
    List<TrainingConfig> vulnerabilityTrainingConfigs =
        trainerConfigServiceConfig.getVulnerabilityTrainingConfigs();
    List<TrainingActionConfig> trainingActionConfigs =
        trainerConfigServiceConfig.getDefaultTrainingActionConfigs();
    testApiNamingTrainingConfigs(apiNamingTrainingConfigs);
    testMetadataTrainingConfigs(metadataTrainingConfigs);
    testVulnerabilityTrainingConfigs(vulnerabilityTrainingConfigs);
    testDefaultTrainingActionConfigs(trainingActionConfigs);
  }

  private void testVulnerabilityTrainingConfigs(List<TrainingConfig> vulnerabilityTrainingConfigs) {
    assertEquals(24, vulnerabilityTrainingConfigs.size());
    vulnerabilityTrainingConfigs.forEach(
        trainingConfig -> {
          assertFalse(trainingConfig.getDisabled());
          assertTrue(trainingConfig.getVulnerabilityTrainingConfig().hasAutoResolutionConfig());
          assertTrue(
              trainingConfig
                  .getVulnerabilityTrainingConfig()
                  .getAutoResolutionConfig()
                  .getDisabled());
        });
  }

  private void testApiNamingTrainingConfigs(List<TrainingConfig> apiNamingTrainingConfigs) {
    assertEquals(2, apiNamingTrainingConfigs.size());

    assertFalse(apiNamingTrainingConfigs.get(0).getDisabled());
    assertEquals(
        List.of("v\\d+"),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getAllowRegexList()
            .getValuesList());
    assertEquals(
        List.of(
            "(\\{){0,1}[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}(\\}){0,1}",
            "\\d+"),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getIds()
            .getRegexList()
            .getValuesList());
    assertEquals(
        List.of(),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getLowCardinality()
            .getRegexList()
            .getValuesList());
    assertEquals(
        List.of("[a-zA-Z]*?([-_+]?[a-zA-Z]+)+[-_+]?"),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getHighCardinality()
            .getRegexList()
            .getValuesList());
    assertEquals(
        List.of("json", "xml"),
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getExtensions()
            .getValuesList());

    assertEquals(
        1,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getIds()
            .getThreshold());
    assertEquals(
        10,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getLowCardinality()
            .getThreshold());
    assertEquals(
        25,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getMediumCardinality()
            .getThreshold());
    assertEquals(
        45,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getHighCardinality()
            .getThreshold());
    assertEquals(
        100,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getEmbryonicThreshold());
    assertEquals(
        10000,
        apiNamingTrainingConfigs
            .get(1)
            .getApiNamingTrainingConfig()
            .getTrieModelTrainingConfig()
            .getMaxNumberOfTriePaths());
  }

  private void testMetadataTrainingConfigs(List<TrainingConfig> metadataTrainingConfigs) {
    assertFalse(metadataTrainingConfigs.get(0).getDisabled());
    ThresholdFamilyConfig digitLengthThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(0)
            .getMetadataTrainingConfig()
            .getDigitLength()
            .getThresholdFamilyConfig();
    ThresholdCountConfig digitLengthDiverseIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig digitLengthDiverseIpLimitedUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig digitLengthLimitedIpFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig digitLengthLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(20, digitLengthDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, digitLengthDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, digitLengthDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, digitLengthDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, digitLengthDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, digitLengthDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, digitLengthLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, digitLengthLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, digitLengthLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, digitLengthLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, digitLengthLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, digitLengthLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(1).getDisabled());
    ThresholdFamilyConfig specialCharsThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(1)
            .getMetadataTrainingConfig()
            .getSpecialChars()
            .getThresholdFamilyConfig();
    ThresholdCountConfig specialCharsDiverseIpDiverseUserFamilyConfig =
        specialCharsThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig specialCharsDiverseIpLimitedUserFamilyConfig =
        specialCharsThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig specialCharsLimitedIpFamilyConfig =
        specialCharsThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig specialCharsLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(20, specialCharsDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, specialCharsDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, specialCharsDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, specialCharsDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, specialCharsDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, specialCharsDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, specialCharsLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, specialCharsLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, specialCharsLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, specialCharsLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, specialCharsLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, specialCharsLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(2).getDisabled());
    ThresholdFamilyConfig htmlTagsThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(2)
            .getMetadataTrainingConfig()
            .getHtmlTags()
            .getThresholdFamilyConfig();
    ThresholdCountConfig htmlTagsDiverseIpDiverseUserFamilyConfig =
        htmlTagsThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig htmlTagsDiverseIpLimitedUserFamilyConfig =
        htmlTagsThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig htmlTagsLimitedIpFamilyConfig =
        htmlTagsThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig htmlTagsLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(20, htmlTagsDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, htmlTagsDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, htmlTagsDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagsDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, htmlTagsDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, htmlTagsDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagsLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, htmlTagsLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, htmlTagsLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagsLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, htmlTagsLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, htmlTagsLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(3).getDisabled());
    ThresholdFamilyConfig htmlTagAttributesThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(3)
            .getMetadataTrainingConfig()
            .getHtmlTagAttributes()
            .getThresholdFamilyConfig();
    ThresholdCountConfig htmlTagAttributesDiverseIpDiverseUserFamilyConfig =
        htmlTagAttributesThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig htmlTagAttributesDiverseIpLimitedUserFamilyConfig =
        htmlTagAttributesThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig htmlTagAttributesLimitedIpFamilyConfig =
        htmlTagAttributesThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig htmlTagAttributesLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(20, htmlTagAttributesDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, htmlTagAttributesDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, htmlTagAttributesDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagAttributesDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, htmlTagAttributesDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, htmlTagAttributesDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagAttributesLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, htmlTagAttributesLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, htmlTagAttributesLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, htmlTagAttributesLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, htmlTagAttributesLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, htmlTagAttributesLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(4).getDisabled());
    ThresholdFamilyConfig httpStatusThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(4)
            .getMetadataTrainingConfig()
            .getHttpStatus()
            .getThresholdFamilyConfig();
    ThresholdCountConfig httpStatusDiverseIpDiverseUserFamilyConfig =
        httpStatusThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig httpStatusDiverseIpLimitedUserFamilyConfig =
        httpStatusThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig httpStatusLimitedIpFamilyConfig =
        httpStatusThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig httpStatusLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(20, httpStatusDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, httpStatusDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, httpStatusDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, httpStatusDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, httpStatusDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, httpStatusDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, httpStatusLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, httpStatusLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, httpStatusLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, httpStatusLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, httpStatusLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, httpStatusLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(5).getDisabled());
    ThresholdFamilyConfig deviceThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(5)
            .getMetadataTrainingConfig()
            .getDevice()
            .getThresholdFamilyConfig();
    ThresholdCountConfig deviceDiverseIpDiverseUserFamilyConfig =
        deviceThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig deviceDiverseIpLimitedUserFamilyConfig =
        deviceThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig deviceLimitedIpFamilyConfig =
        deviceThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig deviceLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(20, deviceDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, deviceDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, deviceDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, deviceDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, deviceDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, deviceDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, deviceLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, deviceLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, deviceLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, deviceLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, deviceLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, deviceLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(6).getDisabled());
    assertEquals(
        20,
        metadataTrainingConfigs.get(6).getMetadataTrainingConfig().getSsrf().getMaxAllowedHosts());
    ThresholdFamilyConfig protocolThresholdFamilyConfig =
        metadataTrainingConfigs
            .get(6)
            .getMetadataTrainingConfig()
            .getSsrf()
            .getProtocolThresholdFamilyConfig();
    ThresholdCountConfig protocolThresholdDiverseIpDiverseUserFamilyConfig =
        protocolThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig protocolThresholdDiverseIpLimitedUserFamilyConfig =
        protocolThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig protocolThresholdLimitedIpFamilyConfig =
        protocolThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig protocolThresholdLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(20, protocolThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        5, protocolThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(5, protocolThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, protocolThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, protocolThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(5, protocolThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, protocolThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, protocolThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, protocolThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, protocolThresholdLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, protocolThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, protocolThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(7).getDisabled());
    ContentSizeTrainingConfig contentSizeTrainingConfig =
        metadataTrainingConfigs.get(7).getMetadataTrainingConfig().getContentSize();
    assertEquals(1000, contentSizeTrainingConfig.getRequestRangeSize());
    assertEquals(1000, contentSizeTrainingConfig.getResponseRangeSize());

    assertFalse(metadataTrainingConfigs.get(8).getDisabled());
    AccessorsTrainingConfig accessorsTrainingConfig =
        metadataTrainingConfigs.get(8).getMetadataTrainingConfig().getAccessors();
    ThresholdFamilyConfig apiThresholdFamilyConfig =
        accessorsTrainingConfig.getApiThresholdFamilyConfig();
    ThresholdCountConfig apiThresholdDiverseIpDiverseUserFamilyConfig =
        apiThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig apiThresholdDiverseIpLimitedUserFamilyConfig =
        apiThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig apiThresholdLimitedIpFamilyConfig =
        apiThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig apiThresholdLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(100, apiThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, apiThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, apiThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, apiThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(0, apiThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(10, apiThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, apiThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, apiThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, apiThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, apiThresholdLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(10, apiThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, apiThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    ThresholdFamilyConfig queryParamThresholdFamilyConfig =
        accessorsTrainingConfig.getQueryParamThresholdFamilyConfig();
    ThresholdCountConfig queryParamThresholdDiverseIpDiverseUserFamilyConfig =
        queryParamThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig queryParamThresholdDiverseIpLimitedUserFamilyConfig =
        queryParamThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig queryParamThresholdLimitedIpFamilyConfig =
        queryParamThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig queryParamThresholdLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(100, queryParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, queryParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, queryParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, queryParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, queryParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, queryParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, queryParamThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, queryParamThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, queryParamThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, queryParamThresholdLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, queryParamThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        0, queryParamThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    ThresholdFamilyConfig requestHeaderThresholdFamilyConfig =
        accessorsTrainingConfig.getRequestHeaderThresholdFamilyConfig();
    ThresholdCountConfig requestHeaderThresholdDiverseIpDiverseUserFamilyConfig =
        requestHeaderThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig requestHeaderThresholdDiverseIpLimitedUserFamilyConfig =
        requestHeaderThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig requestHeaderThresholdLimitedIpFamilyConfig =
        requestHeaderThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig requestHeaderThresholdLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(
        100, requestHeaderThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, requestHeaderThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, requestHeaderThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(
        100, requestHeaderThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, requestHeaderThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, requestHeaderThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, requestHeaderThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, requestHeaderThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, requestHeaderThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(
        20, requestHeaderThresholdLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, requestHeaderThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        0, requestHeaderThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    ThresholdFamilyConfig requestCookieThresholdFamilyConfig =
        accessorsTrainingConfig.getRequestCookieThresholdFamilyConfig();
    ThresholdCountConfig requestCookieThresholdDiverseIpDiverseUserFamilyConfig =
        requestCookieThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig requestCookieThresholdDiverseIpLimitedUserFamilyConfig =
        requestCookieThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig requestCookieThresholdLimitedIpFamilyConfig =
        requestCookieThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig requestCookieThresholdLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(
        100, requestCookieThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, requestCookieThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, requestCookieThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(
        100, requestCookieThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, requestCookieThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        10, requestCookieThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(100, requestCookieThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, requestCookieThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, requestCookieThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(
        20, requestCookieThresholdLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, requestCookieThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        0, requestCookieThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    ThresholdFamilyConfig requestBodyParamThresholdFamilyConfig =
        accessorsTrainingConfig.getRequestBodyParamThresholdFamilyConfig();
    ThresholdCountConfig requestBodyParamThresholdDiverseIpDiverseUserFamilyConfig =
        requestBodyParamThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig requestBodyParamThresholdDiverseIpLimitedUserFamilyConfig =
        requestBodyParamThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig requestBodyParamThresholdLimitedIpFamilyConfig =
        requestBodyParamThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig requestBodyParamThresholdLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(
        50, requestBodyParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        5,
        requestBodyParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        5, requestBodyParamThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(
        50, requestBodyParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0,
        requestBodyParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        5, requestBodyParamThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(50, requestBodyParamThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, requestBodyParamThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, requestBodyParamThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(
        20, requestBodyParamThresholdLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10,
        requestBodyParamThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(
        0, requestBodyParamThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    ThresholdFamilyConfig paramTypeThresholdFamilyConfig =
        accessorsTrainingConfig.getParamTypeThresholdFamilyConfig();
    ThresholdCountConfig paramTypeThresholdDiverseIpDiverseUserFamilyConfig =
        paramTypeThresholdFamilyConfig.getDiverseIpDiverseUserFamilyConfig();
    ThresholdCountConfig paramTypeThresholdDiverseIpLimitedUserFamilyConfig =
        paramTypeThresholdFamilyConfig.getDiverseIpLimitedUserFamilyConfig();
    ThresholdCountConfig paramTypeThresholdLimitedIpFamilyConfig =
        paramTypeThresholdFamilyConfig.getLimitedIpFamilyConfig();
    ThresholdCountConfig paramTypeThresholdLimitedIpDiverseUserFamilyConfig =
        digitLengthThresholdFamilyConfig.getLimitedIpDiverseUserFamilyConfig();
    assertEquals(50, paramTypeThresholdDiverseIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        5, paramTypeThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(5, paramTypeThresholdDiverseIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(50, paramTypeThresholdDiverseIpLimitedUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        0, paramTypeThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(5, paramTypeThresholdDiverseIpLimitedUserFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(50, paramTypeThresholdLimitedIpFamilyConfig.getRequiredCallsCount());
    assertEquals(0, paramTypeThresholdLimitedIpFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, paramTypeThresholdLimitedIpFamilyConfig.getRequiredUniqueIpsCount());
    assertEquals(20, paramTypeThresholdLimitedIpDiverseUserFamilyConfig.getRequiredCallsCount());
    assertEquals(
        10, paramTypeThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueUserIdsCount());
    assertEquals(0, paramTypeThresholdLimitedIpDiverseUserFamilyConfig.getRequiredUniqueIpsCount());

    assertFalse(metadataTrainingConfigs.get(9).getDisabled());
    ThresholdsFamilyTrainingConfig thresholdsFamilyTrainingConfig =
        metadataTrainingConfigs.get(9).getMetadataTrainingConfig().getThresholdsFamily();
    assertEquals(172800000, thresholdsFamilyTrainingConfig.getLearningTimeMillis());
    assertEquals(10, thresholdsFamilyTrainingConfig.getRequiredIpsForDiverseSet());
    assertEquals(10, thresholdsFamilyTrainingConfig.getRequiredUserIdsForDiverseSet());
    assertEquals(60, thresholdsFamilyTrainingConfig.getRequiredPercentForAuthenticated());
    assertEquals(List.of(404), thresholdsFamilyTrainingConfig.getBadStatusIps().getValuesList());
    assertEquals(
        List.of(401, 403, 301, 308),
        thresholdsFamilyTrainingConfig.getBadStatusUserIds().getValuesList());

    assertFalse(metadataTrainingConfigs.get(10).getDisabled());
    EnumerationsTrainingConfig enumerationsTrainingConfig =
        metadataTrainingConfigs.get(10).getMetadataTrainingConfig().getEnum();
    assertEquals(5, enumerationsTrainingConfig.getMaxEnumerations());
    assertEquals(10, enumerationsTrainingConfig.getEnumValueMaxLength());
    assertEquals(99.9, enumerationsTrainingConfig.getMinEnumOccurrencePercent());

    assertFalse(metadataTrainingConfigs.get(11).getDisabled());

    assertTrue(metadataTrainingConfigs.get(12).getDisabled());
    UserAttributionTrainingConfig userAttributionTrainingConfig =
        metadataTrainingConfigs.get(12).getMetadataTrainingConfig().getUserAttribution();
    assertEquals(1000, userAttributionTrainingConfig.getRuleLimit());
    assertEquals(
        List.of("sub", "user", "user_id", "uid", "email", "username", "clientid"),
        userAttributionTrainingConfig.getUserIdKeywords().getValuesList());
    assertEquals(
        List.of("role", "roles", "user_role", "scope"),
        userAttributionTrainingConfig.getUserRoleKeywords().getValuesList());
  }

  private void testDefaultTrainingActionConfigs(List<TrainingActionConfig> trainingActionConfigs) {
    assertEquals(2, trainingActionConfigs.size());
    assertFalse(
        trainingActionConfigs
            .get(0)
            .getTrainingAction()
            .getUserRoleAction()
            .getPauseEntityLearnAction()
            .getDisabledAll());
    assertFalse(
        trainingActionConfigs
            .get(0)
            .getTrainingAction()
            .getUserScopeAction()
            .getPauseEntityLearnAction()
            .getDisabledAll());
  }
}
