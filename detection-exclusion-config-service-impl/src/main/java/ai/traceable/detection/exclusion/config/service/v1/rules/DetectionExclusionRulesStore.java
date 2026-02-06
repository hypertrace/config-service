package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.audit.utils.AuditFilterUtils.hasActiveAuditFilter;
import static ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesUtils.DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesUtils.DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE;
import static ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesUtils.filterConfig;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleRecord;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest;
import org.hypertrace.config.service.v1.UpsertAllConfigsRequest.ConfigToUpsert;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class DetectionExclusionRulesStore
    extends IdentifiedObjectStoreWithFilter<DetectionExclusionRule, GetRulesFilter> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DetectionExclusionRulesStore.class);
  private static final String SSRF_BAD_REPUTATION_HOST = "ssrfBadReputationHost";
  private static final String SSRF_EVENT_TYPE_ID = "ssrf";
  private static final Set<ContextualKey<Void>> SSRF_FIXED_TENANTS = new HashSet<>();

  private final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private final List<DetectionExclusionRuleRecord> defaultDetectionExclusionRuleRecords;
  private final List<DetectionExclusionRuleRecord> defaultNewDetectionExclusionRuleRecords;
  private final FeatureCachingClient featureFlagServiceClient;
  private final DetectionExclusionAuditHelper auditHelper;

  @Inject
  public DetectionExclusionRulesStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureFlagServiceClient,
      DetectionExclusionConfigServiceConfig config,
      DetectionExclusionAuditHelper auditHelper) {
    super(
        configServiceBlockingStub,
        DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE,
        DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.defaultDetectionExclusionRuleRecords =
        config.getDefaultDetectionExclusionRules().stream()
            .map(rule -> DetectionExclusionRuleRecord.newBuilder().setRule(rule).build())
            .collect(Collectors.toUnmodifiableList());
    this.defaultNewDetectionExclusionRuleRecords =
        config.getDefaultNewDetectionExclusionRules().stream()
            .map(rule -> DetectionExclusionRuleRecord.newBuilder().setRule(rule).build())
            .collect(Collectors.toUnmodifiableList());
    this.featureFlagServiceClient = featureFlagServiceClient;
    this.auditHelper = auditHelper;
  }

  @Override
  public Optional<DetectionExclusionRule> getData(RequestContext context, String id) {
    List<DetectionExclusionRuleRecord> defaultRecords = getDefaultRuleRecords(context);
    return super.getData(context, id)
        .or(
            () ->
                defaultRecords.stream()
                    .map(DetectionExclusionRuleRecord::getRule)
                    .filter(rule -> rule.getId().equals(id))
                    .findFirst());
  }

  @Override
  public List<ContextualConfigObject<DetectionExclusionRule>> getAllObjects(
      RequestContext context, GetRulesFilter filter) {
    return super.getAllObjects(context, filter).stream()
        .filter(configObject -> auditHelper.matchesAuditFilters(configObject, filter))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<DetectionExclusionRule> getAllConfigData(RequestContext context) {
    return getAllRuleRecords(context).stream()
        .map(DetectionExclusionRuleRecord::getRule)
        .collect(Collectors.toUnmodifiableList());
  }

  public List<DetectionExclusionRule> getAllConfigDataWithoutDefaults(RequestContext context) {
    return super.getAllConfigData(context);
  }

  @Override
  public List<DetectionExclusionRule> getAllConfigData(
      RequestContext context, GetRulesFilter filter) {
    return getAllRuleRecords(context, filter).stream()
        .map(DetectionExclusionRuleRecord::getRule)
        .collect(Collectors.toUnmodifiableList());
  }

  public List<DetectionExclusionRuleRecord> getAllRuleRecords(RequestContext context) {
    List<DetectionExclusionRuleRecord> storedRecords = getStoredRuleRecords(context, null);
    List<DetectionExclusionRuleRecord> defaultRecords = getDefaultRuleRecords(context);
    return mergeRuleRecords(context, storedRecords, defaultRecords);
  }

  public List<DetectionExclusionRuleRecord> getAllRuleRecords(
      RequestContext context, GetRulesFilter filter) {
    List<DetectionExclusionRuleRecord> storedRecords = getStoredRuleRecords(context, filter);

    // When audit filter is active, don't include default rules (they have no audit data)
    if (hasActiveAuditFilter(filter.getAuditFilter())) {
      return storedRecords;
    }

    List<DetectionExclusionRuleRecord> filteredDefaultRecords =
        getDefaultRuleRecords(context).stream()
            .filter(ruleRecord -> filterConfigData(ruleRecord.getRule(), filter).isPresent())
            .collect(Collectors.toUnmodifiableList());

    return mergeRuleRecords(context, storedRecords, filteredDefaultRecords);
  }

  private List<DetectionExclusionRuleRecord> getDefaultRuleRecords(RequestContext context) {
    return featureFlagServiceClient.isApiProtectConfigPoliciesRevampEnabled(context)
        ? defaultNewDetectionExclusionRuleRecords
        : defaultDetectionExclusionRuleRecords;
  }

  private List<DetectionExclusionRuleRecord> getStoredRuleRecords(
      RequestContext context, GetRulesFilter filter) {
    List<ContextualConfigObject<DetectionExclusionRule>> objects =
        filter != null ? getAllObjects(context, filter) : getAllObjects(context);

    // Build records
    return objects.stream().map(auditHelper::toRuleRecord).collect(Collectors.toUnmodifiableList());
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
    return filterConfig(detectionExclusionRule, filter);
  }

  @Override
  public List<ContextualConfigObject<DetectionExclusionRule>> upsertObjects(
      RequestContext context, List<DetectionExclusionRule> data) {
    List<ConfigToUpsert> configs =
        data.stream()
            .map(
                singleData ->
                    ConfigToUpsert.newBuilder()
                        .setResourceName(DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAME)
                        .setResourceNamespace(DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE)
                        .setContext(this.getContextFromData(singleData))
                        .setConfig(this.buildValueFromData(singleData))
                        .build())
            .collect(Collectors.toUnmodifiableList());

    return context
        .call(
            () ->
                this.configServiceBlockingStub
                    .withDeadline(getDeadline())
                    .upsertAllConfigs(
                        UpsertAllConfigsRequest.newBuilder().addAllConfigs(configs).build()))
        .getUpsertedConfigsList()
        .stream()
        .map(
            upsertedConfig ->
                this.tryBuild(
                    upsertedConfig.getContext(),
                    upsertedConfig.getConfig(),
                    upsertedConfig.getCreationTimestamp(),
                    upsertedConfig.getUpdateTimestamp()))
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private Optional<ContextualConfigObject<DetectionExclusionRule>> tryBuild(
      String context, Value config, long creationTimestamp, long updateTimestamp) {
    return this.buildDataFromValue(config)
        .map(
            data ->
                DetectionExclusionContextualConfig.<DetectionExclusionRule>builder()
                    .context(context)
                    .data(data)
                    .creationTimestamp(Instant.ofEpochMilli(creationTimestamp))
                    .lastUpdatedTimestamp(Instant.ofEpochMilli(updateTimestamp))
                    .build());
  }

  @lombok.Value
  @Builder
  static class DetectionExclusionContextualConfig<T> implements ContextualConfigObject<T> {
    String context;
    T data;
    Instant creationTimestamp;
    Instant lastUpdatedTimestamp;
    String createdByEmail;
    Instant lastUserUpdateTimestamp;
    String lastUserUpdateEmail;
    String lastUpdateEmail;
  }

  private List<DetectionExclusionRuleRecord> mergeRuleRecords(
      RequestContext context,
      List<DetectionExclusionRuleRecord> storedRecords,
      List<DetectionExclusionRuleRecord> defaultRecords) {

    if (!SSRF_FIXED_TENANTS.contains(context.buildInternalContextualKey())) {
      // Bugfix: https://traceableai.atlassian.net/browse/ENG-33151
      storedRecords = fixSsrfRulesIfAny(context, storedRecords);
    }

    Map<String, DetectionExclusionRuleRecord> recordMap = new HashMap<>();
    recordMap.putAll(getRuleIdToRecordMap(defaultRecords));
    recordMap.putAll(getRuleIdToRecordMap(storedRecords));

    return recordMap.values().stream().collect(Collectors.toUnmodifiableList());
  }

  private Map<String, DetectionExclusionRuleRecord> getRuleIdToRecordMap(
      List<DetectionExclusionRuleRecord> records) {
    return records.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                ruleRecord -> ruleRecord.getRule().getId(), Function.identity()));
  }

  private List<DetectionExclusionRuleRecord> fixSsrfRulesIfAny(
      RequestContext context, List<DetectionExclusionRuleRecord> storedRuleRecords) {
    Map<String, DetectionExclusionRuleRecord> ssrfFixedRules =
        storedRuleRecords.stream()
            .filter(
                rule ->
                    rule.getRule().getRuleInfo().getConditionsList().stream()
                        .anyMatch(
                            condition ->
                                condition.getEventCondition().getSystemDefinedEventsList().stream()
                                    .anyMatch(
                                        event ->
                                            SSRF_BAD_REPUTATION_HOST.equals(
                                                event.getEventTypeId()))))
            .map(
                ruleRecord -> {
                  List<DetectionExclusionCondition> conditions =
                      ruleRecord.getRule().getRuleInfo().getConditionsList().stream()
                          .map(
                              condition -> {
                                if (condition.getEventCondition().getSystemDefinedEventsCount()
                                    == 0) {
                                  return condition;
                                }
                                List<SystemDefinedEvent> events =
                                    condition
                                        .getEventCondition()
                                        .getSystemDefinedEventsList()
                                        .stream()
                                        .map(
                                            event -> {
                                              if (SSRF_BAD_REPUTATION_HOST.equals(
                                                  event.getEventTypeId())) {
                                                return event.toBuilder()
                                                    .setEventTypeId(SSRF_EVENT_TYPE_ID)
                                                    .build();
                                              }
                                              return event;
                                            })
                                        .collect(Collectors.toList());
                                return condition.toBuilder()
                                    .setEventCondition(
                                        condition.getEventCondition().toBuilder()
                                            .clearSystemDefinedEvents()
                                            .addAllSystemDefinedEvents(events))
                                    .build();
                              })
                          .collect(Collectors.toList());
                  DetectionExclusionRule fixedSsrfRule =
                      ruleRecord.getRule().toBuilder()
                          .setRuleInfo(
                              ruleRecord.getRule().getRuleInfo().toBuilder()
                                  .clearConditions()
                                  .addAllConditions(conditions))
                          .build();
                  return ruleRecord.toBuilder().setRule(fixedSsrfRule).build();
                })
            .collect(
                Collectors.toMap(ruleRecord -> ruleRecord.getRule().getId(), Function.identity()));

    // This is going to be the majority of the cases
    if (ssrfFixedRules.isEmpty()) {
      return storedRuleRecords;
    }

    LOGGER.info(
        "For tenant:{} fixed 'ssrf' ruleId in exclusion rules [{}]",
        context.getTenantId().get(),
        String.join(",", ssrfFixedRules.keySet()));

    try {
      // upsert the fixed rules
      List<DetectionExclusionRule> fixedRulesToUpsert =
          ssrfFixedRules.values().stream()
              .map(DetectionExclusionRuleRecord::getRule)
              .collect(Collectors.toList());
      upsertObjects(context, fixedRulesToUpsert);
      SSRF_FIXED_TENANTS.add(context.buildInternalContextualKey());
    } catch (Exception e) {
      LOGGER.error(
          "Error in upserting ssrf-fixed exclusion rules for tenant:{},",
          context.getTenantId().get(),
          e);
    }

    return storedRuleRecords.stream()
        .map(ruleRecord -> ssrfFixedRules.getOrDefault(ruleRecord.getRule().getId(), ruleRecord))
        .collect(Collectors.toList());
  }
}
