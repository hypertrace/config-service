package ai.traceable.auth.detection.config.service;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import lombok.SneakyThrows;

class DefaultAuthRuleConfig {
  private static final String DEFAULT_RULE_CONFIG_PATH = "default.auth.detection.rules";
  private final Map<String, AuthDetectionRule> ruleMap;

  DefaultAuthRuleConfig(Config config) {
    // Use guava map to maintain insertion order
    this.ruleMap =
        config.getObjectList(DEFAULT_RULE_CONFIG_PATH).stream()
            .map(this::buildRuleFromConfig)
            .collect(ImmutableMap.toImmutableMap(AuthDetectionRule::getId, Function.identity()));
  }

  Collection<AuthDetectionRule> getAll() {
    return ruleMap.values();
  }

  boolean isDefaultConfig(String id) {
    return ruleMap.containsKey(id);
  }

  @SneakyThrows
  private AuthDetectionRule buildRuleFromConfig(ConfigObject configObject) {
    String jsonString = configObject.render();
    AuthDetectionRule.Builder builder = AuthDetectionRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    builder.setDefault(true);
    return builder.build();
  }
}
