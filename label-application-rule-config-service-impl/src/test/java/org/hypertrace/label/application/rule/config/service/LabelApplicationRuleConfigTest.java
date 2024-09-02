package org.hypertrace.label.application.rule.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LabelApplicationRuleConfigTest {
  private static final String SYSTEM_LABEL_APPLICATION_RULE_STR =
      "{\"id\":\"system-label-application-rule-1\",\"data\":{\"name\":\"SystemLabelApplicationRule1\",\"matching_condition\":{\"leaf_condition\":{\"key_condition\":{\"operator\":\"OPERATOR_EQUALS\",\"value\":\"test.key\"},\"unary_condition\":{\"operator\":\"OPERATOR_EXISTS\"}}},\"label_action\":{\"entity_types\":[\"API\"],\"operation\":\"OPERATION_MERGE\",\"static_labels\":{\"ids\":[\"static-label-id-1\"]}},\"enabled\":false}}";
  LabelApplicationRule systemLabelApplicationRule;
  Map<String, LabelApplicationRule> stringLabelApplicationRuleMap;
  LabelApplicationRuleConfig labelApplicationRuleConfig;

  @BeforeEach
  void setUp() throws InvalidProtocolBufferException {
    String configStr =
        "label.application.rule.config.service {\n"
            + "max.dynamic.label.application.rules.per.tenant = 5\n"
            + "system.label.application.rules = [\n"
            + SYSTEM_LABEL_APPLICATION_RULE_STR
            + "\n]\n"
            + "}\n";
    Config config = ConfigFactory.parseString(configStr);
    LabelApplicationRule.Builder builder = LabelApplicationRule.newBuilder().clear();
    JsonFormat.parser().merge(SYSTEM_LABEL_APPLICATION_RULE_STR, builder);
    systemLabelApplicationRule = builder.build();
    stringLabelApplicationRuleMap = new HashMap<>();
    stringLabelApplicationRuleMap.put(
        systemLabelApplicationRule.getId(), systemLabelApplicationRule);
    labelApplicationRuleConfig = new LabelApplicationRuleConfig(config);
  }

  @Test
  void test_getMaxDynamicLabelApplicationRulesAllowed() {
    assertEquals(5, labelApplicationRuleConfig.getMaxDynamicLabelApplicationRulesAllowed());
  }

  @Test
  void test_getSystemLabelApplicationRules() {
    assertEquals(
        List.of(systemLabelApplicationRule),
        labelApplicationRuleConfig.getSystemLabelApplicationRules());
  }

  @Test
  void test_getSystemLabelApplicationRulesMap() {
    Optional<LabelApplicationRule> rule =
        labelApplicationRuleConfig.getSystemLabelApplicationRule(
            systemLabelApplicationRule.getId());
    assertTrue(rule.isPresent());
    assertEquals(systemLabelApplicationRule, rule.get());
    assertTrue(
        labelApplicationRuleConfig.getSystemLabelApplicationRule("id-does-not-exist").isEmpty());
  }
}
