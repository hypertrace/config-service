package ai.traceable.sensitivedata.config.service;

import static java.util.function.Predicate.not;

import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.ConfigObject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DefaultRedactionRules {
  private final Map<String, NewRedactionRule> ruleMap;

  DefaultRedactionRules(ConfigObject ruleConfigMap) {
    this(
        ruleConfigMap.keySet().stream()
            .sorted()
            .collect(
                ImmutableMap.toImmutableMap( // Predictable iteration order
                    Function.identity(),
                    key ->
                        buildNewRedactionRuleFromConfig(ruleConfigMap.toConfig().getObject(key)))));
  }

  DefaultRedactionRules(Map<String, NewRedactionRule> ruleMap) {
    this.ruleMap = ruleMap;
  }

  public List<RedactionRule> getUnpersistedRules(
      DefaultRedactionRulePersistenceStatus persistenceStatus) {
    return this.ruleMap.keySet().stream()
        .filter(not(persistenceStatus::hasRuleBeenPersisted))
        .map(key -> this.buildRedactionRuleWithId(key, this.ruleMap.get(key)))
        .collect(Collectors.toUnmodifiableList());
  }

  public Optional<RedactionRule> getRule(String id) {
    return Optional.ofNullable(this.ruleMap.get(id))
        .map(newRule -> this.buildRedactionRuleWithId(id, newRule));
  }

  public boolean isDefaultRuleId(String id) {
    return this.ruleMap.containsKey(id);
  }

  @SneakyThrows
  private static NewRedactionRule buildNewRedactionRuleFromConfig(ConfigObject configObject) {
    String jsonString = configObject.render();
    NewRedactionRule.Builder builder = NewRedactionRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }

  private RedactionRule buildRedactionRuleWithId(String id, NewRedactionRule newRedactionRule) {
    RedactionRule.Builder builder =
        RedactionRule.newBuilder()
            .setId(id)
            .setName(newRedactionRule.getName())
            .setDescription(newRedactionRule.getDescription())
            .setCategory(newRedactionRule.getCategory())
            .setRedactionStrategy(newRedactionRule.getRedactionStrategy())
            .setMatchType(newRedactionRule.getMatchType())
            .setRegex(newRedactionRule.getRegex())
            .setSessionIdentifier(newRedactionRule.getSessionIdentifier())
            .addAllConditions(newRedactionRule.getConditionsList())
            .setFqn(newRedactionRule.getFqn());
    if (newRedactionRule.hasComplexData()) {
      builder.setComplexData(newRedactionRule.getComplexData());
    }
    return builder.build();
  }
}
