package ai.traceable.jwt.extraction.config.service;

import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import lombok.SneakyThrows;

class DefaultJwtExtractionRuleConfig {
  private static final String DEFAULT_RULE_CONFIG_PATH = "default.jwt.extraction.rules";
  private final Map<String, JwtExtractionRule> ruleMap;

  DefaultJwtExtractionRuleConfig(Config config) {
    // Use guava map to maintain insertion order
    this.ruleMap =
        config.getObjectList(DEFAULT_RULE_CONFIG_PATH).stream()
            .map(this::buildRuleFromConfig)
            .collect(ImmutableMap.toImmutableMap(JwtExtractionRule::getId, Function.identity()));
  }

  Collection<JwtExtractionRule> getAll() {
    return ruleMap.values();
  }

  boolean isDefaultConfig(String id) {
    return ruleMap.containsKey(id);
  }

  @SneakyThrows
  private JwtExtractionRule buildRuleFromConfig(ConfigObject configObject) {
    String jsonString = configObject.render();
    JwtExtractionRule.Builder builder = JwtExtractionRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    builder.setDefault(true);
    return builder.build();
  }
}
