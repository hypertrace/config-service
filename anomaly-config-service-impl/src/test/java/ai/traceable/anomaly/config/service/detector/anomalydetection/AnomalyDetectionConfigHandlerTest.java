package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.AnomalyDetectionConfigHandler;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistry;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistryImpl;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.detector.*;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.EmailDomainAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.IpTypeAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.MaliciousSourcesRulesAnomalyConfig;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AnomalyDetectionConfigHandlerTest {

  private final ConfigConverter configConverter = new ConfigConverter();
  private final ApiDefinitionRegistry apiDefinitionRegistry =
      new ApiDefinitionRegistryImpl(configConverter);
  private final SessionRulesRegistry sessionRulesRegistry =
      new SessionRulesRegistryImpl(configConverter);
  private final VolumetricRulesRegistry volumetricRulesRegistry =
      new VolumetricRulesRegistryImpl(configConverter);
  private final CredentialStuffingRulesRegistry credentialStuffingRulesRegistry =
      new CredentialStuffingRulesRegistryImpl(configConverter);
  private final AnomalyDetectionConfigHandler detectionConfigConverter =
      new AnomalyDetectionConfigHandler(
          apiDefinitionRegistry,
          sessionRulesRegistry,
          volumetricRulesRegistry,
          credentialStuffingRulesRegistry);

  @Test
  void testModsecConfigConvert() throws InvalidProtocolBufferException {
    AnomalySubRuleConfig subRuleConfig1 =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("subRule1")
            .setCategoryConfig(
                AnomalyCategoryConfig.newBuilder()
                    .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS))
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setInternal(false))
            .setBlockingEnabled(true)
            .build();

    AnomalySubRuleConfig subRuleConfig2 =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("subRule1")
            .setCategoryConfig(
                AnomalyCategoryConfig.newBuilder()
                    .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                    .setEventScoreCategory(
                        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM)
                    .build())
            .setConfigStatus(
                AnomalyConfigStatusChange.newBuilder().setInternal(true).setDisabled(true))
            .build();

    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setApiScope(AnomalyApiScope.newBuilder().setId("api").build())
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .build())
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder().setInternal(true).build())
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("rule")
                                    .addSubRuleConfigs(subRuleConfig1)
                                    .build()))
                    .build())
            .build();

    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW)
                            .build())
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAnomalyRule(
                                ModsecurityAnomalyRuleConfig.newBuilder()
                                    .setAnomalyRuleId("rule")
                                    .addSubRuleConfigs(subRuleConfig2)
                                    .build()))
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setModsecurityAnomalyDetectionConfig(
                        ModsecurityAnomalyDetectionConfig.newBuilder()
                            .setModsecAllDetection(
                                ModsecurityAllDetectionConfig.newBuilder()
                                    .setEnabledOnAllEntrySpans(true)
                                    .setExcludedParams(
                                        StringList.newBuilder()
                                            .addAllValues(List.of("a", "b", "c"))
                                            .build())
                                    .build()))
                    .build())
            .build();

    Value value = detectionConfigConverter.convert(scopedAnomalyDetectionConfig1);

    assertEquals(scopedAnomalyDetectionConfig1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig resultConfig =
        detectionConfigConverter.merge(
            scopedAnomalyDetectionConfig1, scopedAnomalyDetectionConfig2);
    assertEquals("api", resultConfig.getConfigScope().getApiScope().getId());
    assertTrue(
        resultConfig.getAnomalyDetectionConfigsList().get(0).getConfigStatus().getInternal());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getCategoryConfig()
            .getEventScoreCategory());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getCategoryConfig()
            .getEventCategory());
    assertEquals(
        "rule",
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getAnomalyRuleId());

    assertEquals(
        "subRule1",
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getSubRuleId());

    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getCategoryConfig()
            .getEventCategory());

    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM,
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getCategoryConfig()
            .getEventScoreCategory());

    assertTrue(
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getBlockingEnabled());

    assertTrue(
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getConfigStatus()
            .getDisabled());

    assertFalse(
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(0)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAnomalyRule()
            .getSubRuleConfigsList()
            .get(0)
            .getConfigStatus()
            .getInternal());

    assertTrue(
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(1)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAllDetection()
            .getEnabledOnAllEntrySpans());

    assertEquals(
        List.of("a", "b", "c"),
        resultConfig
            .getAnomalyDetectionConfigsList()
            .get(1)
            .getModsecurityAnomalyDetectionConfig()
            .getModsecAllDetection()
            .getExcludedParams()
            .getValuesList());
  }

  @Test
  void testStateBasedDetectionConfigsConvert() throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig config1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder()
                            .setDisabled(true)
                            .setInternal(true)
                            .build())
                    .setApiStateBasedAnomalyDetectionConfig(
                        ApiStateBasedAnomalyDetectionConfig.newBuilder()
                            .setLearntApi(
                                LearntApiAnomalyConfig.newBuilder()
                                    .setModsecurityEnabled(true)
                                    .setEvaluateAllModsecurityRules(true)
                                    .build())
                            .build())
                    .build())
            .build();

    ScopedAnomalyDetectionConfig config2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setApiStateBasedAnomalyDetectionConfig(
                        ApiStateBasedAnomalyDetectionConfig.newBuilder()
                            .setLearntApi(
                                LearntApiAnomalyConfig.newBuilder()
                                    .setEvaluateAllModsecurityRules(false)
                                    .build())
                            .build())
                    .build())
            .build();

    Value value = detectionConfigConverter.convert(config1);
    assertEquals(config1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig mergedConfig = detectionConfigConverter.merge(config2, config1);
    AnomalyDetectionConfig detectionConfig = mergedConfig.getAnomalyDetectionConfigsList().get(0);

    assertTrue(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    assertTrue(
        detectionConfig
            .getApiStateBasedAnomalyDetectionConfig()
            .getLearntApi()
            .getModsecurityEnabled());
    // overridden values
    assertFalse(
        detectionConfig
            .getApiStateBasedAnomalyDetectionConfig()
            .getLearntApi()
            .getEvaluateAllModsecurityRules());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig.getCategoryConfig().getEventCategory());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        detectionConfig.getCategoryConfig().getEventScoreCategory());
  }

  @Test
  void testApiDefinitionDetectionConfigsConvert() throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig config1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder()
                            .setDisabled(true)
                            .setInternal(true)
                            .build())
                    .setApiDefinitionMetadataAnomalyDetectionConfig(
                        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("integer")
                            .setInteger(
                                IntegerAnomalyConfig.newBuilder()
                                    .setMaxLengthDifference(5)
                                    .setThresholdPercent(0.1))
                            .setSubRuleConfigs(
                                AnomalySubRuleConfigMap.newBuilder()
                                    .putSubRuleConfigs(
                                        "sr1",
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("sr1")
                                            .setConfigStatus(
                                                AnomalyConfigStatusChange.newBuilder()
                                                    .setDisabled(false)
                                                    .setInternal(true))
                                            .build())
                                    .putSubRuleConfigs(
                                        "sr2",
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("sr2")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_LATENT))
                                            .build())
                                    .build())
                            .build())
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setApiDefinitionMetadataAnomalyDetectionConfig(
                        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("enum")
                            .setEnum(EnumerationsAnomalyConfig.getDefaultInstance())
                            .build())
                    .build())
            .build();

    ScopedAnomalyDetectionConfig config2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setApiDefinitionMetadataAnomalyDetectionConfig(
                        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setInteger(
                                IntegerAnomalyConfig.newBuilder().setMaxLengthDifference(10))
                            .setSubRuleConfigs(
                                AnomalySubRuleConfigMap.newBuilder()
                                    .putSubRuleConfigs(
                                        "sr1",
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("sr1")
                                            .setConfigStatus(
                                                AnomalyConfigStatusChange.newBuilder()
                                                    .setDisabled(true)
                                                    .setInternal(false))
                                            .build())
                                    .putSubRuleConfigs(
                                        "sr2",
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("sr2")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_MALICIOUS))
                                            .build())
                                    .build())
                            .build())
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder().setInternal(true).setDisabled(false))
                    .setApiDefinitionMetadataAnomalyDetectionConfig(
                        ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("enum")
                            .build())
                    .build())
            .build();

    Value value = detectionConfigConverter.convert(config1);
    assertEquals(config1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig mergedConfig = detectionConfigConverter.merge(config2, config1);

    AnomalyDetectionConfig detectionConfig =
        getAnomalyDetectionConfig(
            mergedConfig.getAnomalyDetectionConfigsList(),
            ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.ENUM);

    assertFalse(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    assertEquals(
        ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.ENUM,
        detectionConfig.getApiDefinitionMetadataAnomalyDetectionConfig().getConfigCase());

    detectionConfig =
        getAnomalyDetectionConfig(
            mergedConfig.getAnomalyDetectionConfigsList(),
            ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.INTEGER);

    assertTrue(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    assertEquals(
        0.1,
        detectionConfig
            .getApiDefinitionMetadataAnomalyDetectionConfig()
            .getInteger()
            .getThresholdPercent());
    // overridden values
    assertEquals(
        10,
        detectionConfig
            .getApiDefinitionMetadataAnomalyDetectionConfig()
            .getInteger()
            .getMaxLengthDifference());
    assertEquals(
        AnomalyConfigStatusChange.newBuilder().setDisabled(true).setInternal(false).build(),
        detectionConfig
            .getApiDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsMap()
            .get("sr1")
            .getConfigStatus());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        detectionConfig
            .getApiDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsMap()
            .get("sr2")
            .getCategoryConfig()
            .getEventCategory());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig.getCategoryConfig().getEventCategory());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        detectionConfig.getCategoryConfig().getEventScoreCategory());
  }

  @Test
  void testSessionDefinitionMetadataDetectionConfigsConvert()
      throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig config1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder()
                            .setDisabled(true)
                            .setInternal(true)
                            .build())
                    .setSessionDefinitionMetadataAnomalyDetectionConfig(
                        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setObjectBola(
                                ObjectBolaAnomalyConfig.newBuilder()
                                    .setMinCorrelationProbability(0.5)
                                    .setAnySourceCorrelationProbability(0.6)
                                    .build())
                            .build())
                    .build())
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW)
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT))
                    .setSessionDefinitionMetadataAnomalyDetectionConfig(
                        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setUserIdBola(UserIdBolaAnomalyConfig.getDefaultInstance())
                            .setSubRuleConfigs(
                                AnomalySubRuleConfigMap.newBuilder()
                                    .putSubRuleConfigs(
                                        "sr1",
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("sr1")
                                            .setConfigStatus(
                                                AnomalyConfigStatusChange.newBuilder()
                                                    .setDisabled(false)
                                                    .setInternal(true))
                                            .build()))
                            .build()))
            .build();
    ScopedAnomalyDetectionConfig config2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH))
                    .setSessionDefinitionMetadataAnomalyDetectionConfig(
                        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setObjectBola(
                                ObjectBolaAnomalyConfig.newBuilder()
                                    .setDisabledForMissingPrecedingParam(true)
                                    .build())))
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setSessionDefinitionMetadataAnomalyDetectionConfig(
                        SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("userIdBola")
                            .setUserIdBola(
                                UserIdBolaAnomalyConfig.newBuilder()
                                    .setMinCorrelationProbability(0.8))
                            .setSubRuleConfigs(
                                AnomalySubRuleConfigMap.newBuilder()
                                    .putSubRuleConfigs(
                                        "sr2",
                                        AnomalySubRuleConfig.newBuilder()
                                            .setSubRuleId("sr2")
                                            .setCategoryConfig(
                                                AnomalyCategoryConfig.newBuilder()
                                                    .setEventCategory(
                                                        AnomalyEventCategory
                                                            .ANOMALY_EVENT_CATEGORY_LATENT))
                                            .build())
                                    .build())))
            .build();
    Value value = detectionConfigConverter.convert(config1);
    assertEquals(config1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig mergedConfig = detectionConfigConverter.merge(config2, config1);

    AnomalyDetectionConfig detectionConfig =
        getAnomalyDetectionConfig(
            mergedConfig.getAnomalyDetectionConfigsList(),
            SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.OBJECT_BOLA);

    assertTrue(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_MALICIOUS,
        detectionConfig.getCategoryConfig().getEventCategory());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_HIGH,
        detectionConfig.getCategoryConfig().getEventScoreCategory());
    assertEquals(
        SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.OBJECT_BOLA,
        detectionConfig.getSessionDefinitionMetadataAnomalyDetectionConfig().getConfigCase());
    assertEquals(
        0.5,
        detectionConfig
            .getSessionDefinitionMetadataAnomalyDetectionConfig()
            .getObjectBola()
            .getMinCorrelationProbability());
    assertEquals(
        0.6,
        detectionConfig
            .getSessionDefinitionMetadataAnomalyDetectionConfig()
            .getObjectBola()
            .getAnySourceCorrelationProbability());
    assertTrue(
        detectionConfig
            .getSessionDefinitionMetadataAnomalyDetectionConfig()
            .getObjectBola()
            .getDisabledForMissingPrecedingParam());

    detectionConfig =
        getAnomalyDetectionConfig(
            mergedConfig.getAnomalyDetectionConfigsList(),
            SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.USER_ID_BOLA);

    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig.getCategoryConfig().getEventCategory());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        detectionConfig.getCategoryConfig().getEventScoreCategory());
    assertEquals(
        "userIdBola",
        detectionConfig.getSessionDefinitionMetadataAnomalyDetectionConfig().getAnomalyRuleId());
    assertEquals(
        0.8,
        detectionConfig
            .getSessionDefinitionMetadataAnomalyDetectionConfig()
            .getUserIdBola()
            .getMinCorrelationProbability());
    assertEquals(
        AnomalyConfigStatusChange.newBuilder().setDisabled(false).setInternal(true).build(),
        detectionConfig
            .getSessionDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsMap()
            .get("sr1")
            .getConfigStatus());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig
            .getSessionDefinitionMetadataAnomalyDetectionConfig()
            .getSubRuleConfigs()
            .getSubRuleConfigsMap()
            .get("sr2")
            .getCategoryConfig()
            .getEventCategory());
  }

  @Test
  void testBlockingDetectionConfigsConvert() throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig config1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder()
                            .setDisabled(true)
                            .setInternal(true)
                            .build())
                    .setBlockingMetadataAnomalyDetectionConfig(
                        BlockingMetadataAnomalyDetectionConfig.newBuilder()
                            .setCustomIp(CustomIpAnomalyConfig.getDefaultInstance())
                            .build())
                    .build())
            .build();

    ScopedAnomalyDetectionConfig config2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setBlockingMetadataAnomalyDetectionConfig(
                        BlockingMetadataAnomalyDetectionConfig.newBuilder()
                            .setCustomIp(CustomIpAnomalyConfig.getDefaultInstance())
                            .build())
                    .build())
            .build();

    Value value = detectionConfigConverter.convert(config1);
    assertEquals(config1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig mergedConfig = detectionConfigConverter.merge(config2, config1);
    AnomalyDetectionConfig detectionConfig = mergedConfig.getAnomalyDetectionConfigsList().get(0);

    assertTrue(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    // overridden values
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig.getCategoryConfig().getEventCategory());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        detectionConfig.getCategoryConfig().getEventScoreCategory());
  }

  @Test
  void testCustomRulesDetectionConfigsConvert() throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig config1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder()
                            .setDisabled(true)
                            .setInternal(true)
                            .build())
                    .setCustomRulesAnomalyDetectionConfig(
                        CustomRulesAnomalyDetectionConfig.newBuilder()
                            .setMaliciousSources(
                                MaliciousSourcesRulesAnomalyConfig.newBuilder()
                                    .setEmailDomain(
                                        EmailDomainAnomalyConfig.newBuilder()
                                            .setCriticalEmailFraudScoreMinThreshold(90))
                                    .setIpType(
                                        IpTypeAnomalyConfig.newBuilder()
                                            .setIpReputationScoreMinThreshold(90)))))
            .build();

    ScopedAnomalyDetectionConfig config2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setCustomRulesAnomalyDetectionConfig(
                        CustomRulesAnomalyDetectionConfig.newBuilder()
                            .setMaliciousSources(
                                MaliciousSourcesRulesAnomalyConfig.newBuilder()
                                    .setEmailDomain(
                                        EmailDomainAnomalyConfig.newBuilder()
                                            .setCriticalEmailFraudScoreMinThreshold(95)
                                            .setHighEmailFraudScoreMinThreshold(90))
                                    .setIpType(
                                        IpTypeAnomalyConfig.newBuilder()
                                            .setAbuseVelocityMinThreshold(
                                                AbuseVelocity.ABUSE_VELOCITY_MEDIUM)))))
            .build();

    Value value = detectionConfigConverter.convert(config1);
    assertEquals(config1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig mergedConfig = detectionConfigConverter.merge(config2, config1);
    AnomalyDetectionConfig detectionConfig = mergedConfig.getAnomalyDetectionConfigsList().get(0);

    assertTrue(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig.getCategoryConfig().getEventCategory());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        detectionConfig.getCategoryConfig().getEventScoreCategory());

    EmailDomainAnomalyConfig emailDomainAnomalyConfig =
        detectionConfig
            .getCustomRulesAnomalyDetectionConfig()
            .getMaliciousSources()
            .getEmailDomain();
    IpTypeAnomalyConfig ipTypeAnomalyConfig =
        detectionConfig.getCustomRulesAnomalyDetectionConfig().getMaliciousSources().getIpType();

    assertEquals(95, emailDomainAnomalyConfig.getCriticalEmailFraudScoreMinThreshold());
    assertEquals(90, emailDomainAnomalyConfig.getHighEmailFraudScoreMinThreshold());
    assertEquals(
        AbuseVelocity.ABUSE_VELOCITY_MEDIUM, ipTypeAnomalyConfig.getAbuseVelocityMinThreshold());
    assertEquals(90, ipTypeAnomalyConfig.getIpReputationScoreMinThreshold());
  }

  @Test
  void testVolumetricDetectionConfigConvert() throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig config1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder()
                            .setDisabled(true)
                            .setInternal(true)
                            .build())
                    .setVolumetricAnomalyDetectionConfig(
                        VolumetricAnomalyDetectionConfig.newBuilder()
                            .setApiCallSpike(ApiCallSpikeAnomalyConfig.newBuilder().build()))
                    .build())
            .build();

    ApiCallSpikeTuningConfig apiCallSpikeTuningConfig =
        ApiCallSpikeTuningConfig.newBuilder()
            .setDetectionScopeConfig(
                DetectionScopeConfig.newBuilder()
                    .setEndpointLabels(StringList.newBuilder().addValues("volumetric-label")))
            .setEndpointSpanCountDetectionThreshold(100)
            .build();
    ScopedAnomalyDetectionConfig config2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setVolumetricAnomalyDetectionConfig(
                        VolumetricAnomalyDetectionConfig.newBuilder()
                            .setApiCallSpike(
                                ApiCallSpikeAnomalyConfig.newBuilder()
                                    .setApiCallSpikeTuningConfigList(
                                        ApiCallSpikeTuningConfigList.newBuilder()
                                            .addApiCallSpikeTuningConfigs(
                                                apiCallSpikeTuningConfig)))))
            .build();

    ScopedAnomalyDetectionConfig config3 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setVolumetricAnomalyDetectionConfig(
                        VolumetricAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("volumetricApiCallSpike")
                            .build()))
            .build();

    Value value = detectionConfigConverter.convert(config1);
    assertEquals(config1, detectionConfigConverter.convert(value));

    ScopedAnomalyDetectionConfig mergedConfig1 = detectionConfigConverter.merge(config2, config1);
    AnomalyDetectionConfig detectionConfig1 = mergedConfig1.getAnomalyDetectionConfigsList().get(0);

    assertTrue(detectionConfig1.getConfigStatus().getDisabled());
    assertTrue(detectionConfig1.getConfigStatus().getInternal());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig1.getCategoryConfig().getEventCategory());
    assertEquals(
        AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW,
        detectionConfig1.getCategoryConfig().getEventScoreCategory());
    List<ApiCallSpikeTuningConfig> apiCallSpikeTuningConfigs =
        detectionConfig1
            .getVolumetricAnomalyDetectionConfig()
            .getApiCallSpike()
            .getApiCallSpikeTuningConfigList()
            .getApiCallSpikeTuningConfigsList();
    assertEquals(1, apiCallSpikeTuningConfigs.size());
    assertEquals(apiCallSpikeTuningConfig, apiCallSpikeTuningConfigs.get(0));

    ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder =
        ScopedAnomalyDetectionConfig.newBuilder();
    deletedConfigBuilder.setConfigScope(config1.getConfigScope());

    List<AnomalyDetectionConfig> detectionConfigsToDelete = new ArrayList<>();
    detectionConfigsToDelete.add(config1.getAnomalyDetectionConfigsList().get(0));

    ScopedAnomalyDetectionConfig deleteConfig =
        detectionConfigConverter.deleteWholeAnomalyDetectionConfigs(
            config1, detectionConfigsToDelete, deletedConfigBuilder);
    assertEquals(0, deleteConfig.getAnomalyDetectionConfigsList().size());

    ScopedAnomalyDetectionConfig mergedConfig2 = detectionConfigConverter.merge(config3, config1);
    AnomalyDetectionConfig detectionConfig2 = mergedConfig2.getAnomalyDetectionConfigsList().get(0);
    assertTrue(detectionConfig2.getConfigStatus().getDisabled());
    assertTrue(detectionConfig2.getConfigStatus().getInternal());
  }

  @Test
  void testCredentialStuffingDetectionConfigsConvert() throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig config1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setConfigStatus(
                        AnomalyConfigStatusChange.newBuilder()
                            .setDisabled(true)
                            .setInternal(true)
                            .build())
                    .setCredentialAnomalyDetectionConfig(
                        CredentialStuffingAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("credentialstuffing")
                            .setCredentialStuffing(
                                CredentialStuffingAnomalyConfig.getDefaultInstance())))
            .build();

    CredentialStuffingTuningConfig credentialStuffingTuningConfig =
        CredentialStuffingTuningConfig.newBuilder()
            .setDetectionScopeConfig(
                DetectionScopeConfig.newBuilder()
                    .setUrlRegexes(StringList.newBuilder().addValues(".*(?i)(login|signin).*")))
            .setUsernameExtractionConfig(
                ParameterExtractionConfig.newBuilder()
                    .setDataTypeIds(
                        StringList.newBuilder()
                            .addValues("data-type-id-1")
                            .addValues("data-type-id-2")))
            .setLookBackDuration(Duration.newBuilder().setSeconds(1000))
            .setUniqueUsersThreshold(5)
            .setFailedLoginPercentageThreshold(99)
            .build();
    ScopedAnomalyDetectionConfig config2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setCredentialAnomalyDetectionConfig(
                        CredentialStuffingAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("credentialstuffing")
                            .setCredentialStuffing(
                                CredentialStuffingAnomalyConfig.newBuilder()
                                    .setCredentialStuffingTuningConfigList(
                                        CredentialStuffingTuningConfigList.newBuilder()
                                            .addCredentialStuffingTuningConfigs(
                                                credentialStuffingTuningConfig))))
                    .build())
            .build();

    ScopedAnomalyDetectionConfig config3 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .addAnomalyDetectionConfigs(
                AnomalyDetectionConfig.newBuilder()
                    .setCategoryConfig(
                        AnomalyCategoryConfig.newBuilder()
                            .setEventCategory(AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT)
                            .setEventScoreCategory(
                                AnomalyEventScoreCategory.ANOMALY_EVENT_SCORE_CATEGORY_LOW))
                    .setCredentialAnomalyDetectionConfig(
                        CredentialStuffingAnomalyDetectionConfig.newBuilder()
                            .setAnomalyRuleId("credentialstuffing")
                            .build())
                    .build())
            .build();
    Value value = detectionConfigConverter.convert(config1);
    assertEquals(config1, detectionConfigConverter.convert(value));
    ScopedAnomalyDetectionConfig mergedConfig = detectionConfigConverter.merge(config2, config1);
    AnomalyDetectionConfig detectionConfig = mergedConfig.getAnomalyDetectionConfigsList().get(0);
    assertTrue(detectionConfig.getConfigStatus().getDisabled());
    assertTrue(detectionConfig.getConfigStatus().getInternal());
    assertEquals(
        AnomalyEventCategory.ANOMALY_EVENT_CATEGORY_LATENT,
        detectionConfig.getCategoryConfig().getEventCategory());
    List<CredentialStuffingTuningConfig> credentialStuffingTuningConfigs =
        detectionConfig
            .getCredentialAnomalyDetectionConfig()
            .getCredentialStuffing()
            .getCredentialStuffingTuningConfigList()
            .getCredentialStuffingTuningConfigsList();
    assertEquals(1, credentialStuffingTuningConfigs.size());
    assertEquals(credentialStuffingTuningConfig, credentialStuffingTuningConfigs.get(0));

    ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder =
        ScopedAnomalyDetectionConfig.newBuilder();
    deletedConfigBuilder.setConfigScope(config1.getConfigScope());
    List<AnomalyDetectionConfig> detectionConfigsToDelete = new ArrayList<>();
    detectionConfigsToDelete.add(config1.getAnomalyDetectionConfigsList().get(0));
    ScopedAnomalyDetectionConfig deleteConfig =
        detectionConfigConverter.deleteWholeAnomalyDetectionConfigs(
            config1, detectionConfigsToDelete, deletedConfigBuilder);
    assertEquals(0, deleteConfig.getAnomalyDetectionConfigsList().size());

    ScopedAnomalyDetectionConfig mergedConfig2 = detectionConfigConverter.merge(config3, config1);
    AnomalyDetectionConfig detectionConfig2 = mergedConfig2.getAnomalyDetectionConfigsList().get(0);
    assertTrue(detectionConfig2.getConfigStatus().getDisabled());
    assertTrue(detectionConfig2.getConfigStatus().getInternal());
  }

  private AnomalyDetectionConfig getAnomalyDetectionConfig(
      List<AnomalyDetectionConfig> detectionConfigs,
      ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig
          .getApiDefinitionMetadataAnomalyDetectionConfig()
          .getConfigCase()
          .equals(configCase)) return detectionConfig;
    }
    return null;
  }

  private AnomalyDetectionConfig getAnomalyDetectionConfig(
      List<AnomalyDetectionConfig> detectionConfigs,
      SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase) {
    for (AnomalyDetectionConfig detectionConfig : detectionConfigs) {
      if (detectionConfig
          .getSessionDefinitionMetadataAnomalyDetectionConfig()
          .getConfigCase()
          .equals(configCase)) return detectionConfig;
    }
    return null;
  }
}
