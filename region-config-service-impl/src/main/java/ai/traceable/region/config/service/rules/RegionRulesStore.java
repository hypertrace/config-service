package ai.traceable.region.config.service.rules;

import static ai.traceable.audit.utils.AuditDetailsBuilder.buildAuditDetails;
import static ai.traceable.audit.utils.AuditFilterUtils.matchesAuditFilters;
import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_NAMESPACE;
import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.region.config.service.RegionConfigServiceConfig;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleRecord;
import ai.traceable.region.config.service.v1.RuleScope;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RegionRulesStore
    extends IdentifiedObjectStoreWithFilter<RegionRule, GetRegionRulesFilter> {

  private final RegionConfigServiceConfig regionConfigServiceConfig;

  @Inject
  public RegionRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RegionConfigServiceConfig regionConfigServiceConfig) {
    super(
        configServiceBlockingStub,
        REGION_RULE_CONFIG_NAMESPACE,
        REGION_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.regionConfigServiceConfig = regionConfigServiceConfig;
  }

  @Override
  protected Optional<RegionRule> buildDataFromValue(Value value) {
    try {
      RegionRule.Builder builder = RegionRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error("Parsing the value {} into RegionRule failed with an exception", value, exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(RegionRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(RegionRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<RegionRule> filterConfigData(RegionRule data, GetRegionRulesFilter filter) {
    return Optional.of(data)
        .filter(rule -> !(filter.hasDisabled() && rule.getDisabled() != filter.getDisabled()))
        .filter(rule -> !(filter.hasInternal() && rule.getInternal() != filter.getInternal()))
        .filter(rule -> !filter.hasHidden() || filter.getHidden() == rule.getHidden())
        .filter(
            rule ->
                filter.getRuleActionTypesCount() == 0
                    || filter.getRuleActionTypesList().contains(rule.getActionType()))
        .filter(rule -> filterRuleOnScope(rule, filter.getRuleScope()));
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(RegionRule ruleData, RuleScope filterScope) {
    List<String> ruleEnvironmentIds =
        ruleData.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    if (!filterScope.hasEnvironmentScope() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentIds = filterScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }

  public List<RegionRuleRecord> getRuleRecords(
      RequestContext context, GetRegionRulesFilter filter) {
    boolean isDefaultFilter = GetRegionRulesFilter.getDefaultInstance().equals(filter);
    return getAllObjects(context).stream()
        .filter(ruleWithContext -> isDefaultFilter || matchesFilter(ruleWithContext, filter))
        .map(this::toRuleRecord)
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean matchesFilter(
      ContextualConfigObject<RegionRule> ruleWithContext, GetRegionRulesFilter filter) {
    return filterConfigData(ruleWithContext.getData(), filter).isPresent()
        && matchesAuditFilters(
            ruleWithContext,
            filter.getAuditFilter(),
            this.regionConfigServiceConfig.getUserVisibleEmailConfig());
  }

  private RegionRuleRecord toRuleRecord(ContextualConfigObject<RegionRule> contextual) {
    return RegionRuleRecord.newBuilder()
        .setRule(contextual.getData())
        .setAuditDetails(
            buildAuditDetails(
                contextual, this.regionConfigServiceConfig.getUserVisibleEmailConfig()))
        .build();
  }
}
