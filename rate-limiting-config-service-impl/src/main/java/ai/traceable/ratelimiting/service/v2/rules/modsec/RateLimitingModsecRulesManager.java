package ai.traceable.ratelimiting.service.v2.rules.modsec;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter.RuleAction;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter.Builder;
import ai.traceable.ratelimiting.config.service.v2.ModsecBlobData;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ModsecBlobDataConverter;
import com.google.inject.Inject;
import java.time.Clock;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class RateLimitingModsecRulesManager {
  private final ModsecBlobDataConverter modsecBlobDataConverter;
  private final ModsecRulesRegistry modsecRulesRegistry;
  private final Clock clock;

  @Inject
  public RateLimitingModsecRulesManager(
      ModsecBlobDataConverter modsecBlobDataConverter,
      ModsecRulesRegistry modsecRulesRegistry,
      Clock clock) {
    this.modsecBlobDataConverter = modsecBlobDataConverter;
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.clock = clock;
  }

  public GetRateLimitingRuleModsecRulesResponse getRateLimitingModsecRules(
      RequestContext requestContext,
      GetRateLimitingModsecRulesFilter filter,
      Function<GetRateLimitingRulesFilter, List<RateLimitingRule>> rateLimitingRulesSupplier) {
    List<EnrichedRateLimitingModsecRule> enrichedRateLimitingRules =
        getEnrichedRateLimitingRules(requestContext, filter, rateLimitingRulesSupplier);

    List<ModsecBlobData> serviceScopedModsecBlobData;
    if (filter.getServiceNamesList().isEmpty()) {
      serviceScopedModsecBlobData =
          List.of(
              modsecBlobDataConverter.generateModsecBlobData(
                  enrichedRateLimitingRules, Optional.empty()));
    } else {
      Map<String, List<EnrichedRateLimitingModsecRule>> serviceIdToRuleIdsMap =
          getServiceIdToRulesMap(enrichedRateLimitingRules, filter.getServiceNamesList());

      serviceScopedModsecBlobData =
          filter.getServiceNamesList().stream()
              .filter(serviceIdToRuleIdsMap::containsKey)
              .map(
                  serviceName ->
                      modsecBlobDataConverter.generateModsecBlobData(
                          serviceIdToRuleIdsMap.get(serviceName), Optional.of(serviceName)))
              .collect(Collectors.toUnmodifiableList());
    }

    return GetRateLimitingRuleModsecRulesResponse.newBuilder()
        .setModsecDirectivesBlob(
            modsecRulesRegistry.getModsecHeader(
                ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE))
        .addAllModsecBlobsData(serviceScopedModsecBlobData)
        .addAllRules(
            enrichedRateLimitingRules.stream()
                .map(EnrichedRateLimitingModsecRule::getRule)
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  private List<EnrichedRateLimitingModsecRule> getEnrichedRateLimitingRules(
      RequestContext requestContext,
      final GetRateLimitingModsecRulesFilter filter,
      Function<GetRateLimitingRulesFilter, List<RateLimitingRule>> rateLimitingRulesSupplier) {
    return rateLimitingRulesSupplier.apply(convertFilter(filter)).stream()
        .filter(rule -> filterByRuleAction(rule, filter.getRuleActionsList()))
        .filter(this::filterExpired)
        .map(EnrichedRateLimitingModsecRule::new)
        .filter(
            enrichedRateLimitingModsecRule ->
                filterRulesByServiceScope(
                    enrichedRateLimitingModsecRule.getServiceNames(), filter.getServiceNamesList()))
        .collect(Collectors.toUnmodifiableList());
  }

  private GetRateLimitingRulesFilter convertFilter(final GetRateLimitingModsecRulesFilter filter) {
    Builder filterBuilder =
        GetRateLimitingRulesFilter.newBuilder()
            .addAllCategories(filter.getCategoriesList())
            .setScope(filter.getScope());
    if (filter.hasDisabled()) {
      filterBuilder.setDisabled(filter.getDisabled());
    }
    return filterBuilder.build();
  }

  private boolean filterByRuleAction(
      final RateLimitingRule rule, final List<RuleAction> ruleActions) {
    if (ruleActions.isEmpty()) {
      return true;
    }

    Action actionConfig = rule.getData().getTransactionActionConfig().getAction();
    if (actionConfig.hasBlock()) {
      return ruleActions.contains(RuleAction.RULE_ACTION_TRANSACTION_BLOCKED);
    } else if (actionConfig.hasAllow()) {
      return ruleActions.contains(RuleAction.RULE_ACTION_TRANSACTION_ALLOWED);
    }
    return false;
  }

  private boolean filterExpired(final RateLimitingRule rule) {
    if (rule.getData().getTransactionActionConfig().hasExpirationTimestampMillis()) {
      return rule.getData().getTransactionActionConfig().getExpirationTimestampMillis()
          > clock.millis();
    }
    return true;
  }

  private boolean filterRulesByServiceScope(
      final List<String> ruleServiceScopes, final List<String> serviceNames) {
    if (ruleServiceScopes.isEmpty() || serviceNames.isEmpty()) {
      return true;
    }
    ruleServiceScopes.retainAll(serviceNames);
    return !ruleServiceScopes.isEmpty();
  }

  private static Map<String, List<EnrichedRateLimitingModsecRule>> getServiceIdToRulesMap(
      Collection<EnrichedRateLimitingModsecRule> enrichedRateLimitingRules,
      List<String> filterServiceNames) {
    return enrichedRateLimitingRules.stream()
        .flatMap(
            rule -> {
              if (rule.getServiceNames().isEmpty()) {
                return filterServiceNames.stream()
                    .map(serviceName -> new AbstractMap.SimpleEntry<>(serviceName, rule));
              } else {
                return rule.getServiceNames().stream()
                    .map(serviceName -> new AbstractMap.SimpleEntry<>(serviceName, rule));
              }
            })
        .collect(
            Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList())));
  }
}
