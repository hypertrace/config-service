package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RateLimitingRulesStore
    extends IdentifiedObjectStoreWithFilter<RateLimitingRule, GetRateLimitingRulesFilter> {
  Logger log = LoggerFactory.getLogger(RateLimitingRulesManager.class);
  private final List<RateLimitingRule> defaultRateLimitingRules;

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
    this.defaultRateLimitingRules = config.getDefaultRateLimitingRules();
  }

  @Override
  public Optional<RateLimitingRule> getData(RequestContext context, String id) {
    return super.getData(context, id)
        .or(
            () ->
                defaultRateLimitingRules.stream()
                    .filter(rule -> rule.getId().equals(id))
                    .findFirst());
  }

  @Override
  public List<RateLimitingRule> getAllConfigData(RequestContext context) {
    List<RateLimitingRule> rateLimitingRules = super.getAllConfigData(context);
    return mergeRateLimitingRules(rateLimitingRules, defaultRateLimitingRules);
  }

  @Override
  public List<RateLimitingRule> getAllConfigData(
      RequestContext context, GetRateLimitingRulesFilter filter) {
    List<RateLimitingRule> filteredDefaultRateLimitingRules =
        defaultRateLimitingRules.stream()
            .filter(rule -> filterConfigData(rule, filter).isPresent())
            .collect(Collectors.toUnmodifiableList());
    List<RateLimitingRule> rateLimitingRules = super.getAllConfigData(context, filter);
    return mergeRateLimitingRules(rateLimitingRules, filteredDefaultRateLimitingRules);
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
            rule -> filterRuleOnRuleEvaluationPoints(rule, filter.getRuleEvaluationPointsList()));
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

  private List<RateLimitingRule> mergeRateLimitingRules(
      List<RateLimitingRule> rateLimitingRules, List<RateLimitingRule> defaultRateLimitingRules) {
    Map<String, RateLimitingRule> rateLimitingRuleMap = new HashMap<>();
    rateLimitingRuleMap.putAll(this.getRuleIdToRuleMap(defaultRateLimitingRules));
    rateLimitingRuleMap.putAll(this.getRuleIdToRuleMap(rateLimitingRules));
    return rateLimitingRuleMap.values().stream().collect(Collectors.toUnmodifiableList());
  }

  private Map<String, RateLimitingRule> getRuleIdToRuleMap(
      List<RateLimitingRule> rateLimitingRules) {
    return Maps.uniqueIndex(rateLimitingRules, RateLimitingRule::getId);
  }
}
