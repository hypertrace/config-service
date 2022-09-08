package ai.traceable.iprange.config.service.rules;

import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_CONFIG_NAMESPACE;
import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.RuleScope;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class IpRangeRulesStore
    extends IdentifiedObjectStoreWithFilter<IpRangeRule, GetRulesFilter> {

  @Inject
  public IpRangeRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        IPRANGE_RULE_CONFIG_NAMESPACE,
        IPRANGE_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<IpRangeRule> buildDataFromValue(Value value) {
    try {
      IpRangeRule.Builder builder = IpRangeRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error("Parsing the value {} into IpRangeRule failed with an exception", value, exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(IpRangeRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(IpRangeRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<IpRangeRule> filterConfigData(IpRangeRule ruleData, GetRulesFilter filter) {
    return Optional.of(ruleData)
        .filter(
            rule -> filter.getRuleIdsCount() == 0 || filter.getRuleIdsList().contains(rule.getId()))
        .filter(
            rule ->
                !(filter.hasRuleAction()
                    && rule.getRuleDetails().getRuleAction() != filter.getRuleAction()))
        .filter(rule -> !(filter.hasDisabled() && rule.getDisabled() != filter.getDisabled()))
        .filter(rule -> !(filter.hasInternal() && rule.getInternal() != filter.getInternal()))
        .filter(rule -> filterRuleOnScope(rule, filter.getRuleScope()));
  }

  private boolean filterRuleOnScope(IpRangeRule ruleData, RuleScope filterScope) {
    List<String> filterEnvironmentIds = filterScope.getEnvironmentScope().getEnvironmentIdsList();
    List<String> ruleEnvironmentIds =
        ruleData.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();

    if (filterEnvironmentIds.isEmpty() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }
}
