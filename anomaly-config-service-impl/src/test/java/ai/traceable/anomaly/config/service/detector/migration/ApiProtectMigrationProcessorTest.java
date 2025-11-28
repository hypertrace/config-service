package ai.traceable.anomaly.config.service.detector.migration;

import static ai.traceable.anomaly.config.service.v1.detector.IpType.IP_TYPE_BOT;
import static ai.traceable.anomaly.config.service.v1.detector.IpType.IP_TYPE_TOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.detector.AbuseVelocity;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.BflaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.IpTypeList;
import ai.traceable.anomaly.config.service.v1.detector.JwtAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.LandSpeedViolationConfig;
import ai.traceable.anomaly.config.service.v1.detector.MissingParamAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.MultiValuedStringParamRule;
import ai.traceable.anomaly.config.service.v1.detector.MultiValuedStringParamRulesList;
import ai.traceable.anomaly.config.service.v1.detector.ObjectBolaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionViolationConfig;
import ai.traceable.anomaly.config.service.v1.detector.SsrfAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.UserIdBolaAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.UserIdData;
import ai.traceable.anomaly.config.service.v1.detector.UserIdDataList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ApiProtectMigrationProcessorTest {

  private ApiProtectMigrationProcessor processor;

  @BeforeEach
  void setUp() {
    processor = new ApiProtectMigrationProcessor();
  }

  @Test
  void testComplexMigration() {
    ScopedAnomalyDetectionConfig originalConfig = createComplexSourceConfig();
    ScopedAnomalyDetectionConfig migratedConfig = processor.migrateConfig(originalConfig);
    verifyMigratedConfig(migratedConfig);
    ScopedAnomalyDetectionConfig reMigratedConfig = processor.migrateConfig(migratedConfig);
    assertEquals(migratedConfig, reMigratedConfig);
  }

  @Test
  void testEmptyMigration() {
    ScopedAnomalyDetectionConfig originalConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.getDefaultInstance()))
            .build();
    ScopedAnomalyDetectionConfig migratedConfig = processor.migrateConfig(originalConfig);
    assertEquals(originalConfig, migratedConfig);
  }

  private ScopedAnomalyDetectionConfig createComplexSourceConfig() {
    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(creteObjectBolaConfig())
        .addAnomalyDetectionConfigs(createSsrfConfig())
        .addAnomalyDetectionConfigs(createJwtConfig())
        .addAnomalyDetectionConfigs(createBflaConfig())
        .addAnomalyDetectionConfigs(createMissingParamConfig())
        .addAnomalyDetectionConfigs(createUserIdBolaConfig())
        .addAnomalyDetectionConfigs(createSessionLandSpeedConfig())
        .build();
  }

  private AnomalyDetectionConfig creteObjectBolaConfig() {

    return AnomalyDetectionConfig.newBuilder()
        .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
        .setSessionDefinitionMetadataAnomalyDetectionConfig(
            SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("bola")
                .setObjectBola(
                    ObjectBolaAnomalyConfig.newBuilder()
                        .setMultiValuedStringParamRules(
                            MultiValuedStringParamRulesList.newBuilder()
                                .addRules(
                                    MultiValuedStringParamRule.newBuilder()
                                        .setValueRegex("valueRegex")
                                        .setValueDelimiter("valueDelimiter")
                                        .setKeyRegex("keyRegex")))))
        .build();
  }

  private AnomalyDetectionConfig createSsrfConfig() {
    SsrfAnomalyConfig ssrfAnomalyConfig =
        SsrfAnomalyConfig.newBuilder()
            .setAllowedDomains(
                StringList.newBuilder().addAllValues(List.of("trusted.com", "example.com")).build())
            .build();
    return AnomalyDetectionConfig.newBuilder()
        .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
        .setApiDefinitionMetadataAnomalyDetectionConfig(
            ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("ssrf")
                .setSsrf(ssrfAnomalyConfig))
        .build();
  }

  private AnomalyDetectionConfig createJwtConfig() {
    JwtAnomalyConfig jwtConfig =
        JwtAnomalyConfig.newBuilder()
            .setMinPercentSeen(0.75)
            .setTimeDifferenceBufferMillis(60000)
            .build();
    AnomalySubRuleConfig jwtExpSubRule =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("jwt_exp")
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)
            .build();

    AnomalySubRuleConfig jwtAlgSubRule =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("jwt_alg")
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE)
            .build();

    AnomalySubRuleConfig jwtAud =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("jwt_aud")
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .build();

    AnomalySubRuleConfigMap subRuleConfigMap =
        AnomalySubRuleConfigMap.newBuilder()
            .putSubRuleConfigs("jwt_exp", jwtExpSubRule)
            .putSubRuleConfigs("jwt_alg", jwtAlgSubRule)
            .putSubRuleConfigs("jwt_aud", jwtAud)
            .build();

    return AnomalyDetectionConfig.newBuilder()
        .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
        .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
        .setApiDefinitionMetadataAnomalyDetectionConfig(
            ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("jwt")
                .setJwt(jwtConfig)
                .setSubRuleConfigs(subRuleConfigMap))
        .build();
  }

  private AnomalyDetectionConfig createBflaConfig() {
    BflaAnomalyConfig bflaConfig = BflaAnomalyConfig.newBuilder().setMinPercentSeen(0.8).build();

    return AnomalyDetectionConfig.newBuilder()
        .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
        .setApiDefinitionMetadataAnomalyDetectionConfig(
            ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("bfla")
                .setBfla(bflaConfig))
        .build();
  }

  private AnomalyDetectionConfig createMissingParamConfig() {
    MissingParamAnomalyConfig missingParamConfig =
        MissingParamAnomalyConfig.newBuilder().setMinPercentSeen(0.9).build();

    return AnomalyDetectionConfig.newBuilder()
        .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
        .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
        .setApiDefinitionMetadataAnomalyDetectionConfig(
            ApiDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("missingParam")
                .setMissingParam(missingParamConfig))
        .build();
  }

  private AnomalyDetectionConfig createUserIdBolaConfig() {
    UserIdBolaAnomalyConfig userIdBolaConfig =
        UserIdBolaAnomalyConfig.newBuilder()
            .setMinCorrelationProbability(0.95)
            .setUserIdDataList(
                UserIdDataList.newBuilder()
                    .addAllUserIdData(
                        List.of(
                            UserIdData.newBuilder().setUserIdCustomAttribute("userId").build(),
                            UserIdData.newBuilder().setUserAttributionExtractedId(false).build(),
                            UserIdData.newBuilder()
                                .setUserAttributionExtractedIdFromJwt(true)
                                .build()))
                    .build())
            .build();

    return AnomalyDetectionConfig.newBuilder()
        .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
        .setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance())
        .setSessionDefinitionMetadataAnomalyDetectionConfig(
            SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("userIdBola")
                .setUserIdBola(userIdBolaConfig))
        .build();
  }

  private AnomalyDetectionConfig createSessionLandSpeedConfig() {
    SessionViolationConfig sessionViolationConfig =
        SessionViolationConfig.newBuilder()
            .setLandSpeed(
                LandSpeedViolationConfig.newBuilder()
                    .setIpTypes(
                        IpTypeList.newBuilder()
                            .addAllValues(List.of(IP_TYPE_TOR, IP_TYPE_BOT))
                            .build())
                    .setMinIpAbuseVelocityValue(23)
                    .setMinIpAbuseVelocity(AbuseVelocity.ABUSE_VELOCITY_HIGH)
                    .build())
            .setDisallowUnauthenticatedSessions(true)
            .build();

    return AnomalyDetectionConfig.newBuilder()
        .setSessionDefinitionMetadataAnomalyDetectionConfig(
            SessionDefinitionMetadataAnomalyDetectionConfig.newBuilder()
                .setAnomalyRuleId("sessionv")
                .setSessionViolation(sessionViolationConfig))
        .build();
  }

  private void verifyMigratedConfig(ScopedAnomalyDetectionConfig migratedConfig) {
    assertEquals(
        7,
        migratedConfig.getAnomalyDetectionConfigsList().stream()
            .filter(config -> !config.hasApiProtectAnomalyDetectionConfig())
            .count());
    List<AnomalyDetectionConfig> apiProtectConfigs =
        migratedConfig.getAnomalyDetectionConfigsList().stream()
            .filter(AnomalyDetectionConfig::hasApiProtectAnomalyDetectionConfig)
            .collect(Collectors.toList());
    assertFalse(apiProtectConfigs.isEmpty());

    // Verify JWT rule and config params
    ApiProtectAnomalyRuleConfig jwtRule = findApiProtectRule(apiProtectConfigs, "jwt");
    assertNotNull(jwtRule);
    assertTrue(jwtRule.getSubRuleConfigsCount() > 0);

    AnomalySubRuleConfig jwtExpRule = findSubRule(jwtRule, "jwt_exp");
    assertNotNull(jwtExpRule);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, jwtExpRule.getAnomalyRuleAction());
    assertTrue(jwtExpRule.getConfigParamsMap().containsKey("time_difference_buffer_millis"));
    assertEquals(
        60000.0,
        jwtExpRule.getConfigParamsMap().get("time_difference_buffer_millis").getNumberValue(),
        0.001);

    AnomalySubRuleConfig jwtAlgRule = findSubRule(jwtRule, "jwt_alg");
    assertNotNull(jwtAlgRule);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, jwtAlgRule.getAnomalyRuleAction());
    assertTrue(jwtAlgRule.getConfigParamsMap().containsKey("min_percent_seen"));
    assertEquals(
        0.75, jwtAlgRule.getConfigParamsMap().get("min_percent_seen").getNumberValue(), 0.001);

    AnomalySubRuleConfig jwtAudRule = findSubRule(jwtRule, "jwt_aud");
    assertNotNull(jwtAudRule);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, jwtAudRule.getAnomalyRuleAction());
    assertTrue(jwtAudRule.getConfigParamsMap().containsKey("min_percent_seen"));
    assertEquals(
        0.75, jwtAudRule.getConfigParamsMap().get("min_percent_seen").getNumberValue(), 0.001);

    AnomalySubRuleConfig jwtIssRule = findSubRule(jwtRule, "jwt_iss");
    assertNotNull(jwtIssRule);
    assertTrue(jwtIssRule.getConfigParamsMap().containsKey("min_percent_seen"));
    assertEquals(
        0.75, jwtIssRule.getConfigParamsMap().get("min_percent_seen").getNumberValue(), 0.001);

    // Verify schema validation rule and config params
    ApiProtectAnomalyRuleConfig schemaRule =
        findApiProtectRule(apiProtectConfigs, "schemaValidation");
    assertNotNull(schemaRule);
    AnomalySubRuleConfig schemaMreqpRule = findSubRule(schemaRule, "schemaValidation_mreqp");
    assertNotNull(schemaMreqpRule);
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, schemaMreqpRule.getAnomalyRuleAction());
    assertTrue(schemaMreqpRule.getConfigParamsMap().containsKey("min_percent_seen"));
    assertEquals(
        0.9, schemaMreqpRule.getConfigParamsMap().get("min_percent_seen").getNumberValue(), 0.001);

    // Verify SSRF rule and config params
    ApiProtectAnomalyRuleConfig ssrfRule = findApiProtectRule(apiProtectConfigs, "ssrf");
    assertNotNull(ssrfRule);
    AnomalySubRuleConfig ssrfUhRule = findSubRule(ssrfRule, "ssrf_uh");
    assertNotNull(ssrfUhRule);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, ssrfUhRule.getAnomalyRuleAction());
    assertTrue(ssrfUhRule.getConfigParamsMap().containsKey("allowed_domains"));
    assertEquals(
        2, ssrfUhRule.getConfigParamsMap().get("allowed_domains").getListValue().getValuesCount());
    assertEquals(
        "trusted.com",
        ssrfUhRule
            .getConfigParamsMap()
            .get("allowed_domains")
            .getListValue()
            .getValues(0)
            .getStringValue());
    assertEquals(
        "example.com",
        ssrfUhRule
            .getConfigParamsMap()
            .get("allowed_domains")
            .getListValue()
            .getValues(1)
            .getStringValue());

    AnomalySubRuleConfig ssrfMhRule = findSubRule(ssrfRule, "ssrf_mh");
    assertNotNull(ssrfMhRule);
    assertEquals(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, ssrfMhRule.getAnomalyRuleAction());
    assertTrue(ssrfMhRule.getConfigParamsMap().containsKey("allowed_domains"));
    assertEquals(
        2, ssrfMhRule.getConfigParamsMap().get("allowed_domains").getListValue().getValuesCount());

    // Verify authn rule and config params
    ApiProtectAnomalyRuleConfig authnRule = findApiProtectRule(apiProtectConfigs, "authn");
    assertNotNull(authnRule);

    // Verify authzv rule and config params
    ApiProtectAnomalyRuleConfig authzvRule = findApiProtectRule(apiProtectConfigs, "authzv");
    assertNotNull(authzvRule);
    assertTrue(authzvRule.getSubRuleConfigsCount() > 0);

    AnomalySubRuleConfig authzvBflaRule = findSubRule(authzvRule, "authzv_bfla");
    assertNotNull(authzvBflaRule);
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE, authzvBflaRule.getAnomalyRuleAction());
    assertTrue(authzvBflaRule.getConfigParamsMap().containsKey("min_percent_seen"));
    assertEquals(
        0.8, authzvBflaRule.getConfigParamsMap().get("min_percent_seen").getNumberValue(), 0.001);

    // Verify authzv rule and config params
    ApiProtectAnomalyRuleConfig authzhRule = findApiProtectRule(apiProtectConfigs, "authzh");
    assertNotNull(authzhRule);
    assertTrue(authzhRule.getSubRuleConfigsCount() > 0);

    AnomalySubRuleConfig authzhObolaRule = findSubRule(authzhRule, "authzh_obola");
    assertNotNull(authzhObolaRule);
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, authzhObolaRule.getAnomalyRuleAction());
    assertTrue(authzhObolaRule.getConfigParamsMap().containsKey("multi_valued_string_param_rules"));
    assertEquals(
        1,
        authzhObolaRule
            .getConfigParamsMap()
            .get("multi_valued_string_param_rules")
            .getListValue()
            .getValuesCount());
    assertEquals(
        "keyRegex",
        authzhObolaRule
            .getConfigParamsMap()
            .get("multi_valued_string_param_rules")
            .getListValue()
            .getValues(0)
            .getStructValue()
            .getFieldsMap()
            .get("key_regex")
            .getStringValue());
    assertEquals(
        "valueDelimiter",
        authzhObolaRule
            .getConfigParamsMap()
            .get("multi_valued_string_param_rules")
            .getListValue()
            .getValues(0)
            .getStructValue()
            .getFieldsMap()
            .get("value_delimiter")
            .getStringValue());
    assertEquals(
        "valueRegex",
        authzhObolaRule
            .getConfigParamsMap()
            .get("multi_valued_string_param_rules")
            .getListValue()
            .getValues(0)
            .getStructValue()
            .getFieldsMap()
            .get("value_regex")
            .getStringValue());

    AnomalySubRuleConfig authzhUbolaRule = findSubRule(authzhRule, "authzh_ubola");
    assertNotNull(authzhUbolaRule);
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, authzhUbolaRule.getAnomalyRuleAction());
    assertTrue(authzhUbolaRule.getConfigParamsMap().containsKey("min_correlation_probability"));
    assertEquals(
        0.95,
        authzhUbolaRule.getConfigParamsMap().get("min_correlation_probability").getNumberValue(),
        0.001);
    assertTrue(authzhUbolaRule.getConfigParamsMap().containsKey("user_id_data_list"));
    assertEquals(
        3,
        authzhUbolaRule
            .getConfigParamsMap()
            .get("user_id_data_list")
            .getListValue()
            .getValuesCount());
    assertEquals(
        "userId",
        authzhUbolaRule
            .getConfigParamsMap()
            .get("user_id_data_list")
            .getListValue()
            .getValues(0)
            .getStructValue()
            .getFieldsMap()
            .get("user_id_custom_attribute")
            .getStringValue());
    assertFalse(
        authzhUbolaRule
            .getConfigParamsMap()
            .get("user_id_data_list")
            .getListValue()
            .getValues(1)
            .getStructValue()
            .getFieldsMap()
            .get("user_attribution_extracted_id")
            .getBoolValue());
    assertTrue(
        authzhUbolaRule
            .getConfigParamsMap()
            .get("user_id_data_list")
            .getListValue()
            .getValues(2)
            .getStructValue()
            .getFieldsMap()
            .get("user_attribution_extracted_id_from_jwt")
            .getBoolValue());

    // Verify csta rule and config params
    ApiProtectAnomalyRuleConfig cstaRule = findApiProtectRule(apiProtectConfigs, "csta");
    assertNotNull(cstaRule);
    assertTrue(cstaRule.getSubRuleConfigsCount() > 0);

    AnomalySubRuleConfig cstaCsrfRule = findSubRule(cstaRule, "csta_csrf");
    assertNotNull(cstaCsrfRule);
    assertEquals(
        AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR, cstaCsrfRule.getAnomalyRuleAction());
    assertTrue(cstaCsrfRule.getConfigParamsMap().containsKey("min_percent_seen"));
    assertEquals(
        0.9, cstaCsrfRule.getConfigParamsMap().get("min_percent_seen").getNumberValue(), 0.001);

    // Verify session violation rule and config params
    ApiProtectAnomalyRuleConfig sessionvRule = findApiProtectRule(apiProtectConfigs, "sessionv");
    assertNotNull(sessionvRule);
    AnomalySubRuleConfig sessionvLandspeedRule = findSubRule(sessionvRule, "sessionv_landspeed");
    assertNotNull(sessionvLandspeedRule);
    assertTrue(
        sessionvLandspeedRule
            .getConfigParamsMap()
            .containsKey("disallow_unauthenticated_sessions"));
    assertTrue(
        sessionvLandspeedRule
            .getConfigParamsMap()
            .get("disallow_unauthenticated_sessions")
            .getBoolValue());
    assertTrue(sessionvLandspeedRule.getConfigParamsMap().containsKey("ip_types"));
    assertEquals(
        2,
        sessionvLandspeedRule.getConfigParamsMap().get("ip_types").getListValue().getValuesCount());
    assertEquals(
        "IP_TYPE_TOR",
        sessionvLandspeedRule
            .getConfigParamsMap()
            .get("ip_types")
            .getListValue()
            .getValues(0)
            .getStringValue());
    assertEquals(
        "IP_TYPE_BOT",
        sessionvLandspeedRule
            .getConfigParamsMap()
            .get("ip_types")
            .getListValue()
            .getValues(1)
            .getStringValue());
    assertTrue(sessionvLandspeedRule.getConfigParamsMap().containsKey("min_ip_abuse_velocity"));
    assertEquals(
        "ABUSE_VELOCITY_HIGH",
        sessionvLandspeedRule.getConfigParamsMap().get("min_ip_abuse_velocity").getStringValue());

    AnomalySubRuleConfig sessionvExpiryRule = findSubRule(sessionvRule, "sessionv_expiry");
    assertNotNull(sessionvExpiryRule);
    assertTrue(
        sessionvExpiryRule.getConfigParamsMap().containsKey("disallow_unauthenticated_sessions"));
    assertTrue(
        sessionvExpiryRule
            .getConfigParamsMap()
            .get("disallow_unauthenticated_sessions")
            .getBoolValue());
  }

  private ApiProtectAnomalyRuleConfig findApiProtectRule(
      List<AnomalyDetectionConfig> configs, String ruleId) {
    return configs.stream()
        .filter(AnomalyDetectionConfig::hasApiProtectAnomalyDetectionConfig)
        .map(config -> config.getApiProtectAnomalyDetectionConfig().getApiProtectAnomalyRule())
        .filter(rule -> rule.getAnomalyRuleId().equals(ruleId))
        .findFirst()
        .orElse(null);
  }

  private AnomalySubRuleConfig findSubRule(ApiProtectAnomalyRuleConfig rule, String subRuleId) {
    return rule.getSubRuleConfigsList().stream()
        .filter(subRule -> subRule.getSubRuleId().equals(subRuleId))
        .findFirst()
        .orElse(null);
  }

  @Test
  public void testNoChangeWhenNoDetectionConfigs() {
    ScopedAnomalyDetectionConfig emptyConfig = ScopedAnomalyDetectionConfig.newBuilder().build();
    ScopedAnomalyDetectionConfig result = processor.migrateConfig(emptyConfig);
    assertEquals(emptyConfig, result);
  }
}
