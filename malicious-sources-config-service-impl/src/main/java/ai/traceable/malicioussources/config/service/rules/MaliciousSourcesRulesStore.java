package ai.traceable.malicioussources.config.service.rules;

import static ai.traceable.audit.utils.AuditDetailsBuilder.buildAuditDetails;
import static ai.traceable.audit.utils.AuditFilterUtils.matchesAuditFilters;
import static ai.traceable.malicioussources.config.service.constants.MaliciousSourcesConfigConstants.MALICIOUS_SOURCES_RULE_CONFIG_NAMESPACE;
import static ai.traceable.malicioussources.config.service.constants.MaliciousSourcesConfigConstants.MALICIOUS_SOURCES_RULE_CONFIG_RESOURCE_NAME;

import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleRecord;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class MaliciousSourcesRulesStore
    extends IdentifiedObjectStoreWithFilter<MaliciousSourcesRule, GetRulesFilter> {

  private final MaliciousSourcesConfigServiceConfig serviceConfig;

  @Inject
  public MaliciousSourcesRulesStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      MaliciousSourcesConfigServiceConfig serviceConfig) {
    super(
        configServiceBlockingStub,
        MALICIOUS_SOURCES_RULE_CONFIG_NAMESPACE,
        MALICIOUS_SOURCES_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.serviceConfig = serviceConfig;
  }

  @Override
  protected Optional<MaliciousSourcesRule> buildDataFromValue(Value value) {
    try {
      MaliciousSourcesRule.Builder builder = MaliciousSourcesRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into MaliciousSourcesRule failed with an exception",
          value,
          exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(MaliciousSourcesRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(MaliciousSourcesRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<MaliciousSourcesRule> filterConfigData(
      MaliciousSourcesRule ruleData, GetRulesFilter filter) {
    return Optional.of(ruleData)
        .filter(
            rule -> filter.getRuleIdsCount() == 0 || filter.getRuleIdsList().contains(rule.getId()))
        .filter(rule -> filterRuleOnScope(rule, filter.getRuleScope()))
        .filter(
            rule ->
                !(filter.hasDisabled()
                    && rule.getRuleStatus().getDisabled() != filter.getDisabled()))
        .filter(
            rule -> !filter.hasHidden() || filter.getHidden() == rule.getRuleStatus().getHidden())
        .filter(
            rule ->
                filter.getRuleActionTypesCount() == 0
                    || filter
                        .getRuleActionTypesList()
                        .contains(rule.getRuleInfo().getRuleAction().getActionType()));
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(
      MaliciousSourcesRule ruleData, MaliciousSourcesRuleScope filterScope) {
    List<String> ruleEnvironmentIds =
        ruleData.getRuleScope().getEnvironmentScope().getEnvironmentIdsList();
    if (!filterScope.hasEnvironmentScope() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentIds = filterScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }

  public List<MaliciousSourcesRuleRecord> getRuleRecords(
      RequestContext context, GetRulesFilter filter) {
    boolean isDefaultFilter = GetRulesFilter.getDefaultInstance().equals(filter);
    return getAllObjects(context).stream()
        .filter(ruleWithContext -> isDefaultFilter || matchesFilter(ruleWithContext, filter))
        .map(this::toRuleRecord)
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean matchesFilter(
      ContextualConfigObject<MaliciousSourcesRule> ruleWithContext, GetRulesFilter filter) {
    return filterConfigData(ruleWithContext.getData(), filter).isPresent()
        && matchesAuditFilters(
            ruleWithContext,
            filter.getAuditFilter(),
            this.serviceConfig.getUserVisibleEmailConfig());
  }

  private MaliciousSourcesRuleRecord toRuleRecord(
      ContextualConfigObject<MaliciousSourcesRule> contextual) {
    return MaliciousSourcesRuleRecord.newBuilder()
        .setRule(contextual.getData())
        .setAuditDetails(
            buildAuditDetails(contextual, this.serviceConfig.getUserVisibleEmailConfig()))
        .build();
  }
}
