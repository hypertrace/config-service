package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.ConfigObject;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DefaultRedactionRules {
  private final Map<String, NewRedactionRule> prepopulationRules;
  private final List<NewRedactionRule> defaultRules;

  DefaultRedactionRules(
      ConfigObject prepopulationRuleConfigMap, List<? extends ConfigObject> defaultRules) {
    this(
        prepopulationRuleConfigMap.keySet().stream()
            .sorted()
            .map(
                key ->
                    Map.entry(
                        key,
                        buildRedactionRuleFromConfig(
                            prepopulationRuleConfigMap.toConfig().getObject(key))))
            .collect(
                ImmutableMap.toImmutableMap( // Predictable iteration order
                    Entry::getKey, Entry::getValue)),
        buildRedactionRuleList(defaultRules));
  }

  DefaultRedactionRules(
      Map<String, NewRedactionRule> prepopulationRules, List<NewRedactionRule> defaultRules) {
    this.prepopulationRules = prepopulationRules;
    this.defaultRules = defaultRules;
  }

  public Map<String, NewRedactionRule> getRulesToPrepopulate(
      DefaultRedactionRulePopulationStatus currentStatus) {

    return prepopulationRules.entrySet().stream()
        .filter(entry -> currentStatus.shouldPopulateRule(entry.getKey()))
        .collect(
            ImmutableMap.toImmutableMap(
                Entry::getKey, Entry::getValue)); // Predictable iteration order
  }

  public List<NewRedactionRule> getDefaultRules() {
    return List.copyOf(this.defaultRules);
  }

  boolean isPrepopulationComplete(DefaultRedactionRulePopulationStatus status) {
    return this.prepopulationRules.keySet().stream().allMatch(status::hasRuleBeenPopulated);
  }

  DefaultRedactionRulePopulationStatus completedPrepopulationStatus(
      DefaultRedactionRulePopulationStatus currentStatus) {
    return currentStatus.withAdditionalPopulatedRules(this.prepopulationRules.keySet());
  }

  private static List<NewRedactionRule> buildRedactionRuleList(
      List<? extends ConfigObject> configObjectList) {
    return configObjectList.stream()
        .map(DefaultRedactionRules::buildRedactionRuleFromConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  @SneakyThrows
  private static NewRedactionRule buildRedactionRuleFromConfig(ConfigObject configObject) {
    String jsonString = configObject.render();
    NewRedactionRule.Builder builder = NewRedactionRule.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }
}
