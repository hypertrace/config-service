package ai.traceable.anomaly.config.service.override.exclusion;

import static ai.traceable.anomaly.config.service.override.common.DetectionOverrideConfigConstants.DETECTION_OVERRIDE_EXCLUSION_RULE_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.override.common.DetectionOverrideConfigConstants.DETECTION_OVERRIDE_EXCLUSION_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRule;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideRuleScope;
import ai.traceable.anomaly.config.service.v1.override.GetDetectionExclusionRulesFilter;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class DetectionOverrideRulesStore
    extends IdentifiedObjectStoreWithFilter<
        DetectionExclusionRule, GetDetectionExclusionRulesFilter> {

  @Inject
  public DetectionOverrideRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        DETECTION_OVERRIDE_EXCLUSION_RULE_CONFIG_NAMESPACE,
        DETECTION_OVERRIDE_EXCLUSION_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<DetectionExclusionRule> buildDataFromValue(Value value) {
    try {
      DetectionExclusionRule.Builder builder = DetectionExclusionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      log.error(
          "Parsing the value {} into DetectionExclusionRule failed with an exception", value, e);
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
      DetectionExclusionRule ruleData, GetDetectionExclusionRulesFilter filter) {
    return Optional.of(ruleData).filter(rule -> filterRuleOnScope(rule, filter.getRuleScope()));
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(
      DetectionExclusionRule rule, DetectionOverrideRuleScope filterScope) {
    List<String> ruleEnvironmentIds =
        rule.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    if (!filterScope.hasEnvironmentScope() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentIds = filterScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }
}
