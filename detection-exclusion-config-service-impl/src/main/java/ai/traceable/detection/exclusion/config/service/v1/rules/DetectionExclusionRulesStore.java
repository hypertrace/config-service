package ai.traceable.detection.exclusion.config.service.v1.rules;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class DetectionExclusionRulesStore
    extends IdentifiedObjectStoreWithFilter<DetectionExclusionRule, GetRulesFilter> {
  public static final String DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAME =
      "detectionExclusionRule";
  public static final String DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE =
      "detectionExclusionRuleConfig";

  @Inject
  public DetectionExclusionRulesStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE,
        DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DetectionExclusionRule> buildDataFromValue(Value value) {
    try {
      DetectionExclusionRule.Builder builder = DetectionExclusionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing value {} into DetectionExclusionRule failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DetectionExclusionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(DetectionExclusionRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<DetectionExclusionRule> filterConfigData(
      DetectionExclusionRule detectionExclusionRule, GetRulesFilter filter) {
    return Optional.of(detectionExclusionRule)
        .filter(
            rule ->
                filter.getRuleIdsList().isEmpty()
                    || filter.getRuleIdsList().contains(detectionExclusionRule.getId()))
        .filter(
            rule ->
                !filter.hasDisabled()
                    || filter.getDisabled()
                        == detectionExclusionRule.getRuleInfo().getRuleStatus().getDisabled())
        .filter(
            rule ->
                !filter.hasHidden()
                    || filter.getHidden()
                        == detectionExclusionRule.getRuleInfo().getRuleStatus().getHidden())
        .filter(
            rule ->
                filter.getRuleChangeSourcesList().isEmpty()
                    || filter
                        .getRuleChangeSourcesList()
                        .contains(rule.getRuleInfo().getRuleStatus().getChangeSource()))
        .filter(rule -> filterRuleOnScope(rule, filter.getRuleScope()));
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(
      DetectionExclusionRule rule, DetectionExclusionRuleScope ruleScope) {
    List<String> ruleEnvironmentIds =
        rule.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    if (!ruleScope.hasEnvironmentScope() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentIds = ruleScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }
}
