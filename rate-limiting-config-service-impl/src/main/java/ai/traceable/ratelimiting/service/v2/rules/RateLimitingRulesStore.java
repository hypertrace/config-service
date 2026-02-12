package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.audit.utils.AuditContextualObjectUtils.contextualObjectWithDefaultTraceableAuditInfo;
import static ai.traceable.audit.utils.AuditContextualObjectUtils.enrichWithDefaultAuditInfo;
import static ai.traceable.audit.utils.AuditDetailsBuilder.buildAuditDetails;
import static ai.traceable.audit.utils.AuditFilterUtils.matchesAuditFilters;
import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleRecord;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RateLimitingRulesStore
    extends IdentifiedObjectStoreWithFilter<RateLimitingRule, GetRateLimitingRulesFilter> {
  Logger log = LoggerFactory.getLogger(RateLimitingRulesStore.class);
  private final Map<String, ContextualConfigObject<RateLimitingRule>>
      defaultRateLimitingRuleObjectsMap;
  private final RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig;

  @Inject
  public RateLimitingRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RateLimitingConfigServiceConfig config) {
    super(
        configServiceBlockingStub,
        RATE_LIMITING_RULE_CONFIG_RESOURCE_NAMESPACE,
        RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.defaultRateLimitingRuleObjectsMap =
        buildDefaultRuleObjectsMap(config.getDefaultRateLimitingRules());
    this.rateLimitingConfigServiceConfig = config;
  }

  @Override
  public Optional<ContextualConfigObject<RateLimitingRule>> getObject(
      RequestContext context, String id) {
    return super.getObject(context, id)
        .map(
            contextualObj ->
                enrichWithDefaultAuditInfo(
                    contextualObj, defaultRateLimitingRuleObjectsMap.get(id)))
        .or(() -> Optional.ofNullable(defaultRateLimitingRuleObjectsMap.get(id)));
  }

  @Override
  public List<ContextualConfigObject<RateLimitingRule>> getAllObjects(RequestContext context) {
    List<ContextualConfigObject<RateLimitingRule>> persistedRules = super.getAllObjects(context);

    // Start with defaults, then overlay persisted rules (enriching with default audit info if
    // needed)
    Map<String, ContextualConfigObject<RateLimitingRule>> mergedRulesMap =
        new HashMap<>(defaultRateLimitingRuleObjectsMap);
    persistedRules.forEach(
        rule ->
            mergedRulesMap.put(
                rule.getData().getId(),
                enrichWithDefaultAuditInfo(
                    rule, defaultRateLimitingRuleObjectsMap.get(rule.getData().getId()))));

    return List.copyOf(mergedRulesMap.values());
  }

  @Override
  protected Optional<RateLimitingRule> buildDataFromValue(Value value) {
    try {
      RateLimitingRule.Builder builder = RateLimitingRule.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception exception) {
      log.error(
          "Parsing the value {} into RateLimitingRule failed with an exception", value, exception);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(RateLimitingRule rule) {
    return ConfigProtoConverter.convertToValue(rule);
  }

  @Override
  protected String getContextFromData(RateLimitingRule rule) {
    return rule.getId();
  }

  @Override
  protected Optional<RateLimitingRule> filterConfigData(
      RateLimitingRule rateLimitingRule, GetRateLimitingRulesFilter filter) {
    return Optional.of(rateLimitingRule)
        .filter(rule -> filterRuleOnCategory(rule, filter.getCategoriesList()))
        .filter(rule -> filterRuleOnScope(rule, filter.getScope()))
        .filter(
            rule -> !filter.hasDisabled() || filter.getDisabled() != rule.getData().getEnabled())
        .filter(
            rule -> filterRuleOnRuleEvaluationPoints(rule, filter.getRuleEvaluationPointsList()))
        .filter(rule -> filterRuleOnLabels(rule, filter.getLabelsMap()));
  }

  private boolean filterRuleOnLabels(RateLimitingRule rule, Map<String, String> labels) {
    if (labels.isEmpty()) {
      return true;
    }
    Map<String, String> ruleLabels = rule.getData().getLabelsMap();
    return labels.entrySet().stream()
        .anyMatch(
            entry -> {
              String key = entry.getKey();
              String value = entry.getValue();
              return ruleLabels.containsKey(key)
                  && (StringUtils.isEmpty(value) || ruleLabels.get(key).equals(value));
            });
  }

  private boolean filterRuleOnCategory(RateLimitingRule rule, List<Category> categoryList) {
    return categoryList.isEmpty() || categoryList.contains(rule.getData().getCategory());
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(RateLimitingRule rule, RuleConfigScope filterScope) {
    List<String> ruleEnvironmentIds =
        rule.getData().getRuleConfigScope().getEnvironmentScope().getEnvironmentIdsList();
    if (!filterScope.hasEnvironmentScope() || ruleEnvironmentIds.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentIds = filterScope.getEnvironmentScope().getEnvironmentIdsList();
    return ruleEnvironmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }

  private boolean filterRuleOnRuleEvaluationPoints(
      RateLimitingRule rule, List<RuleEvaluationPoint> ruleEvaluationPointsInFilter) {
    return ruleEvaluationPointsInFilter.isEmpty()
        || rule.getData().getRuleEvaluationPointsList().stream()
            .anyMatch(ruleEvaluationPointsInFilter::contains);
  }

  private Map<String, ContextualConfigObject<RateLimitingRule>> buildDefaultRuleObjectsMap(
      List<RateLimitingRule> defaultRules) {
    return defaultRules.stream()
        .map(rule -> contextualObjectWithDefaultTraceableAuditInfo(rule, rule.getId()))
        .collect(Collectors.toMap(ContextualConfigObject::getContext, Function.identity()));
  }

  public List<RateLimitingRuleRecord> getRuleRecords(
      RequestContext context, GetRateLimitingRulesFilter filter) {
    boolean isDefaultFilter = GetRateLimitingRulesFilter.getDefaultInstance().equals(filter);
    return getAllObjects(context).stream()
        .filter(ruleWithContext -> isDefaultFilter || matchesFilter(ruleWithContext, filter))
        .map(this::toRuleRecord)
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean matchesFilter(
      ContextualConfigObject<RateLimitingRule> ruleWithContext, GetRateLimitingRulesFilter filter) {
    return filterConfigData(ruleWithContext.getData(), filter).isPresent()
        && matchesAuditFilters(
            ruleWithContext,
            filter.getAuditFilter(),
            rateLimitingConfigServiceConfig.getUserVisibleEmailConfig());
  }

  private RateLimitingRuleRecord toRuleRecord(ContextualConfigObject<RateLimitingRule> contextual) {
    return RateLimitingRuleRecord.newBuilder()
        .setRule(contextual.getData())
        .setAuditDetails(
            buildAuditDetails(
                contextual, rateLimitingConfigServiceConfig.getUserVisibleEmailConfig()))
        .build();
  }
}
