package ai.traceable.ratelimiting.service.v2.rules.modsec;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider.ServiceIdentifierEntity;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter.RuleAction;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.ModsecBlobData;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.service.v2.rules.modsec.converters.ModsecBlobDataConverter;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataClassificationInfoProvider;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataClassificationInfoProvider.DataClassificationInfo;
import com.google.inject.Inject;
import java.time.Clock;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
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
  private final DataClassificationInfoProvider dataClassificationInfoProvider;
  private final CachedServiceMappingProvider cachedServiceMappingProvider;
  private final Clock clock;

  @Inject
  public RateLimitingModsecRulesManager(
      ModsecBlobDataConverter modsecBlobDataConverter,
      ModsecRulesRegistry modsecRulesRegistry,
      DataClassificationInfoProvider dataClassificationInfoProvider,
      CachedServiceMappingProvider cachedServiceMappingProvider,
      Clock clock) {
    this.modsecBlobDataConverter = modsecBlobDataConverter;
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.dataClassificationInfoProvider = dataClassificationInfoProvider;
    this.cachedServiceMappingProvider = cachedServiceMappingProvider;
    this.clock = clock;
  }

  public GetRateLimitingRuleModsecRulesResponse getRateLimitingModsecRules(
      RequestContext requestContext,
      GetRateLimitingModsecRulesFilter filter,
      Function<GetRateLimitingRulesFilter, List<RateLimitingRule>> rateLimitingRulesSupplier) {
    DataClassificationInfo dataClassificationInfo =
        dataClassificationInfoProvider.fetchDataClassificationInfo(requestContext);

    List<EnrichedRateLimitingModsecRule> enrichedRateLimitingRules =
        getEnrichedRateLimitingRules(
            filter,
            dataClassificationInfo,
            rateLimitingRulesSupplier,
            getServiceNameProvider(requestContext));
    List<String> environmentIds =
        filter.getRulesFilter().getScope().getEnvironmentScope().getEnvironmentIdsList();

    List<ModsecBlobData> serviceScopedModsecBlobData;
    if (filter.getServiceNamesList().isEmpty()) {
      serviceScopedModsecBlobData =
          modsecBlobDataConverter
              .generateModsecBlobData(
                  enrichedRateLimitingRules,
                  requestContext.getTenantId().orElse(""),
                  Collections.emptyList(),
                  environmentIds)
              .map(Collections::singletonList)
              .orElse(Collections.emptyList());
    } else {
      serviceScopedModsecBlobData =
          getRulesToServiceIdsMap(enrichedRateLimitingRules, filter.getServiceNamesList())
              .entrySet()
              .stream()
              .map(
                  entry ->
                      modsecBlobDataConverter.generateModsecBlobData(
                          entry.getKey(),
                          requestContext.getTenantId().orElse(""),
                          entry.getValue(),
                          environmentIds))
              .flatMap(Optional::stream)
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
      final GetRateLimitingModsecRulesFilter filter,
      DataClassificationInfo dataClassificationInfo,
      Function<GetRateLimitingRulesFilter, List<RateLimitingRule>> rateLimitingRulesSupplier,
      Function<String, String> serviceNameProvider) {
    return rateLimitingRulesSupplier.apply(filter.getRulesFilter()).stream()
        .filter(rule -> filterByRuleAction(rule, filter.getRuleActionsList()))
        .filter(this::filterExpired)
        .map(
            rule ->
                new EnrichedRateLimitingModsecRule(
                    rule, dataClassificationInfo, serviceNameProvider))
        .filter(
            enrichedRateLimitingModsecRule ->
                filterRulesByServiceScope(
                    enrichedRateLimitingModsecRule.getServiceNames(), filter.getServiceNamesList()))
        .collect(Collectors.toUnmodifiableList());
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

  private Function<String, String> getServiceNameProvider(RequestContext requestContext) {
    return serviceId ->
        this.cachedServiceMappingProvider
            .getServiceIdentifierEntity(requestContext, serviceId)
            .map(ServiceIdentifierEntity::getServiceName)
            .orElse(null);
  }

  private static Map<List<EnrichedRateLimitingModsecRule>, List<String>> getRulesToServiceIdsMap(
      List<EnrichedRateLimitingModsecRule> enrichedRateLimitingModsecRules,
      List<String> filterServiceNames) {
    return enrichedRateLimitingModsecRules.stream()
        .flatMap(
            rule ->
                rule.getServiceNames().isEmpty()
                    ? filterServiceNames.stream()
                        .map(serviceName -> new AbstractMap.SimpleEntry<>(serviceName, rule))
                    : rule.getServiceNames().stream()
                        .filter(filterServiceNames::contains)
                        .map(serviceName -> new AbstractMap.SimpleEntry<>(serviceName, rule)))
        .collect(
            Collectors.groupingBy(
                Map.Entry::getKey,
                LinkedHashMap::new, // Maintain insertion order
                Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList())))
        .entrySet()
        .stream()
        .collect(
            Collectors.groupingBy(
                Map.Entry::getValue,
                LinkedHashMap::new,
                Collectors.mapping(
                    Map.Entry::getKey,
                    Collectors.collectingAndThen(
                        Collectors.toList(),
                        sortList ->
                            sortList.stream().sorted().collect(Collectors.toUnmodifiableList())))));
  }
}
