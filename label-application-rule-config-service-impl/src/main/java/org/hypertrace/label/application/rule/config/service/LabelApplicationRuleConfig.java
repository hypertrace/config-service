package org.hypertrace.label.application.rule.config.service;

import static java.util.function.Function.identity;

import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRule;

public class LabelApplicationRuleConfig {
  private static final String LABEL_APPLICATION_RULE_CONFIG_SERVICE_CONFIG =
      "label.application.rule.config.service";
  private static final String MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT =
      "max.dynamic.label.application.rules.per.tenant";
  private static final String SYSTEM_LABEL_APPLICATION_RULES = "system.label.application.rules";
  private static final int DEFAULT_MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT = 100;

  @Getter private final int maxDynamicLabelApplicationRulesAllowed;
  @Getter private final List<LabelApplicationRule> systemLabelApplicationRules;
  private final Map<String, LabelApplicationRule> systemLabelApplicationRulesMap;

  public LabelApplicationRuleConfig(Config config) {
    Config labelApplicationRuleConfig =
        config.hasPath(LABEL_APPLICATION_RULE_CONFIG_SERVICE_CONFIG)
            ? config.getConfig(LABEL_APPLICATION_RULE_CONFIG_SERVICE_CONFIG)
            : ConfigFactory.empty();
    this.maxDynamicLabelApplicationRulesAllowed =
        labelApplicationRuleConfig.hasPath(MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT)
            ? labelApplicationRuleConfig.getInt(MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT)
            : DEFAULT_MAX_DYNAMIC_LABEL_APPLICATION_RULES_PER_TENANT;
    if (labelApplicationRuleConfig.hasPath(SYSTEM_LABEL_APPLICATION_RULES)) {
      final List<? extends ConfigObject> systemLabelApplicationRulesObjectList =
          labelApplicationRuleConfig.getObjectList(SYSTEM_LABEL_APPLICATION_RULES);
      this.systemLabelApplicationRules =
          buildSystemLabelApplicationRuleList(systemLabelApplicationRulesObjectList);
      this.systemLabelApplicationRulesMap =
          this.systemLabelApplicationRules.stream()
              .collect(Collectors.toUnmodifiableMap(LabelApplicationRule::getId, identity()));
    } else {
      this.systemLabelApplicationRules = Collections.emptyList();
      this.systemLabelApplicationRulesMap = Collections.emptyMap();
    }
  }

  private List<LabelApplicationRule> buildSystemLabelApplicationRuleList(
      List<? extends com.typesafe.config.ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(LabelApplicationRuleConfig::buildLabelApplicationRuleFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private static LabelApplicationRule buildLabelApplicationRuleFromConfig(
      com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    LabelApplicationRule.Builder builder = LabelApplicationRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  public Optional<LabelApplicationRule> getSystemLabelApplicationRule(String id) {
    return Optional.ofNullable(systemLabelApplicationRulesMap.get(id));
  }
}
