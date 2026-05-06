package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiProtectionConfigHandlerTest {

  private static final String USER_ROLE_SPAN_FILTER_CONFIG = "user_role_span_filter_config";
  private ApiProtectionConfigHandler handler;

  @BeforeEach
  void setUp() {
    handler = new ApiProtectionConfigHandler();
  }

  @Test
  void merge_shouldReplaceUserRoleSpanFilterConfig_whenPreferredHasNewValue() {
    Value fallbackUserRoleConfig =
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields(
                        "spansWithUserDefinedRoles",
                        Value.newBuilder().setStructValue(Struct.getDefaultInstance()).build()))
            .build();

    Value preferredUserRoleConfig =
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields(
                        "spansWithSpecifiedRoles",
                        Value.newBuilder()
                            .setStructValue(
                                Struct.newBuilder()
                                    .putFields(
                                        "filterMode",
                                        Value.newBuilder()
                                            .setStringValue("USER_ROLE_FILTER_MODE_IN")
                                            .build())
                                    .putFields(
                                        "userRoles",
                                        Value.newBuilder()
                                            .setListValue(
                                                ListValue.newBuilder()
                                                    .addValues(
                                                        Value.newBuilder()
                                                            .setStringValue("admin")
                                                            .build()))
                                            .build()))
                            .build()))
            .build();

    ScopedAnomalyDetectionConfig fallbackConfig =
        buildScopedConfig(
            "authzh",
            "authzh_obola",
            Map.of(
                "sanitize_param_value",
                boolValue(true),
                USER_ROLE_SPAN_FILTER_CONFIG,
                fallbackUserRoleConfig));

    ScopedAnomalyDetectionConfig preferredConfig =
        buildScopedConfig(
            "authzh",
            "authzh_obola",
            Map.of(USER_ROLE_SPAN_FILTER_CONFIG, preferredUserRoleConfig));

    List<AnomalyDetectionConfig> result =
        handler.merge(
            preferredConfig.getAnomalyDetectionConfigsList(),
            fallbackConfig.getAnomalyDetectionConfigsList());

    assertEquals(1, result.size());
    AnomalySubRuleConfig mergedSubRule =
        result
            .get(0)
            .getApiProtectAnomalyDetectionConfig()
            .getApiProtectAnomalyRule()
            .getSubRuleConfigs(0);

    Map<String, Value> configParams = mergedSubRule.getConfigParamsMap();

    // sanitize_param_value from fallback should be preserved
    assertTrue(configParams.containsKey("sanitize_param_value"));
    assertTrue(configParams.get("sanitize_param_value").getBoolValue());

    // user_role_span_filter_config should be REPLACED (not merged)
    Value userRoleValue = configParams.get(USER_ROLE_SPAN_FILTER_CONFIG);
    Map<String, Value> userRoleFields = userRoleValue.getStructValue().getFieldsMap();

    assertTrue(
        userRoleFields.containsKey("spansWithSpecifiedRoles"), "Should have the new oneof key");
    assertFalse(
        userRoleFields.containsKey("spansWithUserDefinedRoles"),
        "Should NOT have the old oneof key - it must be replaced, not merged");
  }

  @Test
  void merge_shouldPreserveOtherConfigParams_whenNonMergeableKeyIsUpdated() {
    ScopedAnomalyDetectionConfig fallbackConfig =
        buildScopedConfig(
            "authzh",
            "authzh_obola",
            Map.of(
                "sanitize_param_value", boolValue(true),
                "min_correlation_probability", numberValue(0.5)));

    ScopedAnomalyDetectionConfig preferredConfig =
        buildScopedConfig(
            "authzh", "authzh_obola", Map.of("sanitize_param_value", boolValue(false)));

    List<AnomalyDetectionConfig> result =
        handler.merge(
            preferredConfig.getAnomalyDetectionConfigsList(),
            fallbackConfig.getAnomalyDetectionConfigsList());

    AnomalySubRuleConfig mergedSubRule =
        result
            .get(0)
            .getApiProtectAnomalyDetectionConfig()
            .getApiProtectAnomalyRule()
            .getSubRuleConfigs(0);

    Map<String, Value> configParams = mergedSubRule.getConfigParamsMap();

    // Updated value should be taken from preferred
    assertFalse(configParams.get("sanitize_param_value").getBoolValue());
    // Untouched value should be preserved from fallback
    assertEquals(0.5, configParams.get("min_correlation_probability").getNumberValue());
  }

  @Test
  void merge_shouldNotModifyMergedConfig_whenPreferredHasNoNonMergeableKeys() {
    Value fallbackUserRoleConfig =
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields(
                        "spansWithUserDefinedRoles",
                        Value.newBuilder().setStructValue(Struct.getDefaultInstance()).build()))
            .build();

    ScopedAnomalyDetectionConfig fallbackConfig =
        buildScopedConfig(
            "authzh",
            "authzh_obola",
            Map.of(
                "sanitize_param_value",
                boolValue(true),
                USER_ROLE_SPAN_FILTER_CONFIG,
                fallbackUserRoleConfig));

    // Preferred only updates sanitize_param_value, not user_role_span_filter_config
    ScopedAnomalyDetectionConfig preferredConfig =
        buildScopedConfig(
            "authzh", "authzh_obola", Map.of("sanitize_param_value", boolValue(false)));

    List<AnomalyDetectionConfig> result =
        handler.merge(
            preferredConfig.getAnomalyDetectionConfigsList(),
            fallbackConfig.getAnomalyDetectionConfigsList());

    AnomalySubRuleConfig mergedSubRule =
        result
            .get(0)
            .getApiProtectAnomalyDetectionConfig()
            .getApiProtectAnomalyRule()
            .getSubRuleConfigs(0);

    Map<String, Value> configParams = mergedSubRule.getConfigParamsMap();

    // user_role_span_filter_config should remain from fallback (deep-merged as before)
    Value userRoleValue = configParams.get(USER_ROLE_SPAN_FILTER_CONFIG);
    assertTrue(
        userRoleValue.getStructValue().getFieldsMap().containsKey("spansWithUserDefinedRoles"));
  }

  @Test
  void merge_shouldHandleNewSubRuleNotInFallback() {
    ScopedAnomalyDetectionConfig fallbackConfig =
        buildScopedConfig(
            "authzh", "authzh_obola", Map.of("sanitize_param_value", boolValue(true)));

    Value userRoleConfig =
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields(
                        "allSpans",
                        Value.newBuilder().setStructValue(Struct.getDefaultInstance()).build()))
            .build();

    // Preferred has a different subRuleId that doesn't exist in fallback
    ScopedAnomalyDetectionConfig preferredConfig =
        buildScopedConfig(
            "authzh", "authzh_ubola", Map.of(USER_ROLE_SPAN_FILTER_CONFIG, userRoleConfig));

    List<AnomalyDetectionConfig> result =
        handler.merge(
            preferredConfig.getAnomalyDetectionConfigsList(),
            fallbackConfig.getAnomalyDetectionConfigsList());

    assertEquals(1, result.size());
    List<AnomalySubRuleConfig> subRules =
        result
            .get(0)
            .getApiProtectAnomalyDetectionConfig()
            .getApiProtectAnomalyRule()
            .getSubRuleConfigsList();

    // Both sub-rules should be present
    assertEquals(2, subRules.size());
  }

  private ScopedAnomalyDetectionConfig buildScopedConfig(
      String ruleId, String subRuleId, Map<String, Value> configParams) {
    AnomalySubRuleConfig.Builder subRuleBuilder =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId(subRuleId)
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR);
    configParams.forEach(subRuleBuilder::putConfigParams);

    return ScopedAnomalyDetectionConfig.newBuilder()
        .addAnomalyDetectionConfigs(
            AnomalyDetectionConfig.newBuilder()
                .setApiProtectAnomalyDetectionConfig(
                    ApiProtectAnomalyDetectionConfig.newBuilder()
                        .setApiProtectAnomalyRule(
                            ApiProtectAnomalyRuleConfig.newBuilder()
                                .setAnomalyRuleId(ruleId)
                                .addSubRuleConfigs(subRuleBuilder.build()))))
        .build();
  }

  private Value boolValue(boolean val) {
    return Value.newBuilder().setBoolValue(val).build();
  }

  private Value numberValue(double val) {
    return Value.newBuilder().setNumberValue(val).build();
  }
}
