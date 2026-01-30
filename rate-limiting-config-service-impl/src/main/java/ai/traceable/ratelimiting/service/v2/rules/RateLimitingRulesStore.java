package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.config.service.commons.utils.AuditFilterUtils.getVisibleUserEmail;
import static ai.traceable.config.service.commons.utils.AuditFilterUtils.hasActiveAuditFilter;
import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.config.commons.v1.AuditDetails;
import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.CreationDetails;
import ai.traceable.config.commons.v1.LastUpdateDetails;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleRecord;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private final List<RateLimitingRule> defaultRateLimitingRules;
  private final TimestampConverter timestampConverter;
  private final RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig;

  @Inject
  public RateLimitingRulesStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RateLimitingConfigServiceConfig config,
      TimestampConverter timestampConverter) {
    super(
        configServiceBlockingStub,
        RATE_LIMITING_RULE_CONFIG_RESOURCE_NAMESPACE,
        RATE_LIMITING_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.defaultRateLimitingRules = config.getDefaultRateLimitingRules();
    this.timestampConverter = timestampConverter;
    this.rateLimitingConfigServiceConfig = config;
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

  public List<RateLimitingRuleRecord> getRuleRecords(
      RequestContext context, GetRateLimitingRulesFilter filter) {
    boolean isDefaultFilter = GetRateLimitingRulesFilter.getDefaultInstance().equals(filter);
    List<RateLimitingRuleRecord> storedRecords =
        getAllObjects(context).stream()
            .filter(ruleWithContext -> isDefaultFilter || matchesFilter(ruleWithContext, filter))
            .map(this::toRuleRecord)
            .collect(Collectors.toList());

    // When audit filter is active, don't include default rules (they have no audit data)
    if (hasActiveAuditFilter(filter.getAuditFilter())) {
      return storedRecords;
    }

    // Add default rules (without audit details since they're not stored)
    List<RateLimitingRuleRecord> defaultRecords =
        defaultRateLimitingRules.stream()
            .filter(rule -> filterConfigData(rule, filter).isPresent())
            .map(rule -> RateLimitingRuleRecord.newBuilder().setRule(rule).build())
            .collect(Collectors.toList());

    return mergeRuleRecords(storedRecords, defaultRecords);
  }

  private List<RateLimitingRuleRecord> mergeRuleRecords(
      List<RateLimitingRuleRecord> storedRecords, List<RateLimitingRuleRecord> defaultRecords) {
    Map<String, RateLimitingRuleRecord> recordMap = new HashMap<>();
    defaultRecords.forEach(ruleRecord -> recordMap.put(ruleRecord.getRule().getId(), ruleRecord));
    storedRecords.forEach(ruleRecord -> recordMap.put(ruleRecord.getRule().getId(), ruleRecord));
    return recordMap.values().stream().collect(Collectors.toUnmodifiableList());
  }

  private boolean matchesFilter(
      ContextualConfigObject<RateLimitingRule> ruleWithContext, GetRateLimitingRulesFilter filter) {
    return filterConfigData(ruleWithContext.getData(), filter).isPresent()
        && matchesAuditFilter(ruleWithContext, filter.getAuditFilter());
  }

  private boolean matchesAuditFilter(
      ContextualConfigObject<RateLimitingRule> contextual, AuditFilter auditFilter) {
    if (AuditFilter.getDefaultInstance().equals(auditFilter)) {
      return true;
    }
    return matchesCreatedRange(contextual, auditFilter)
        && matchesUpdatedRange(contextual, auditFilter)
        && matchesCreatedByContains(contextual, auditFilter)
        && matchesLastUpdatedByContains(contextual, auditFilter);
  }

  private boolean matchesCreatedRange(
      ContextualConfigObject<RateLimitingRule> contextual, AuditFilter auditFilter) {
    if (!auditFilter.hasCreatedRange()) {
      return true;
    }
    Instant creationTimestamp = contextual.getCreationTimestamp();
    if (creationTimestamp == null) {
      return false;
    }
    return isTimestampInRange(creationTimestamp, auditFilter.getCreatedRange());
  }

  private boolean matchesUpdatedRange(
      ContextualConfigObject<RateLimitingRule> contextual, AuditFilter auditFilter) {
    if (!auditFilter.hasUpdatedRange()) {
      return true;
    }
    Instant lastUserUpdateTimestamp = contextual.getLastUserUpdateTimestamp();
    if (lastUserUpdateTimestamp == null) {
      return false;
    }
    return isTimestampInRange(lastUserUpdateTimestamp, auditFilter.getUpdatedRange());
  }

  private boolean matchesCreatedByContains(
      ContextualConfigObject<RateLimitingRule> contextual, AuditFilter auditFilter) {
    String createdByContains = auditFilter.getCreatedByContains();
    if (createdByContains.isEmpty()) {
      return true;
    }
    String createdByEmail = contextual.getCreatedByEmail();
    return createdByEmail != null
        && createdByEmail.toLowerCase().contains(createdByContains.toLowerCase());
  }

  private boolean matchesLastUpdatedByContains(
      ContextualConfigObject<RateLimitingRule> contextual, AuditFilter auditFilter) {
    String lastUpdatedByContains = auditFilter.getLastUpdatedByUserContains();
    if (lastUpdatedByContains.isEmpty()) {
      return true;
    }
    String lastUserUpdateEmail =
        getVisibleUserEmail(
            contextual.getLastUserUpdateEmail(),
            contextual.getLastUpdateEmail(),
            this.rateLimitingConfigServiceConfig.getUserVisibleEmailConfig());
    return lastUserUpdateEmail != null
        && lastUserUpdateEmail.toLowerCase().contains(lastUpdatedByContains.toLowerCase());
  }

  private boolean isTimestampInRange(Instant timestamp, TimestampRange range) {
    if (range.hasStart()) {
      Instant start = timestampConverter.convertToInstant(range.getStart());
      if (timestamp.isBefore(start)) {
        return false;
      }
    }
    if (range.hasEnd()) {
      Instant end = timestampConverter.convertToInstant(range.getEnd());
      if (timestamp.isAfter(end)) {
        return false;
      }
    }
    return true;
  }

  private RateLimitingRuleRecord toRuleRecord(ContextualConfigObject<RateLimitingRule> contextual) {
    return RateLimitingRuleRecord.newBuilder()
        .setRule(contextual.getData())
        .setAuditDetails(buildAuditDetails(contextual))
        .build();
  }

  private AuditDetails buildAuditDetails(ContextualConfigObject<?> contextual) {
    AuditDetails.Builder builder = AuditDetails.newBuilder();

    Instant creationTimestamp = contextual.getCreationTimestamp();
    if (creationTimestamp != null && creationTimestamp.getEpochSecond() > 0) {
      builder.setCreationDetails(
          CreationDetails.newBuilder()
              .setCreatedBy(contextual.getCreatedByEmail())
              .setCreatedAt(timestampConverter.convert(creationTimestamp))
              .build());
    }

    Instant lastUserUpdateTimestamp = contextual.getLastUserUpdateTimestamp();
    if (lastUserUpdateTimestamp != null && lastUserUpdateTimestamp.getEpochSecond() > 0) {
      String userEmail =
          getVisibleUserEmail(
              contextual.getLastUserUpdateEmail(),
              contextual.getLastUpdateEmail(),
              this.rateLimitingConfigServiceConfig.getUserVisibleEmailConfig());
      builder.setLastUserUpdateDetails(
          LastUpdateDetails.newBuilder()
              .setUpdatedBy(userEmail)
              .setUpdatedAt(timestampConverter.convert(lastUserUpdateTimestamp))
              .build());
    }

    return builder.build();
  }
}
