package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesUtils.DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAME;
import static ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesUtils.DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE;
import static ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesUtils.filterConfig;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
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

  private final List<DetectionExclusionRule> defaultDetectionExclusionRules;
  private final List<DetectionExclusionRule> defaultNewDetectionExclusionRules;
  private final FeatureCachingClient featureFlagServiceClient;

  @Inject
  public DetectionExclusionRulesStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      FeatureCachingClient featureFlagServiceClient,
      DetectionExclusionConfigServiceConfig config) {
    super(
        configServiceBlockingStub,
        DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAMESPACE,
        DETECTION_EXCLUSION_RULE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.defaultDetectionExclusionRules = config.getDefaultDetectionExclusionRules();
    this.defaultNewDetectionExclusionRules = config.getDefaultNewDetectionExclusionRules();
    this.featureFlagServiceClient = featureFlagServiceClient;
  }

  @Override
  public Optional<DetectionExclusionRule> getData(RequestContext context, String id) {
    if (featureFlagServiceClient.isApiProtectConfigPoliciesRevampEnabled(context)) {
      return super.getData(context, id)
          .or(
              () ->
                  defaultNewDetectionExclusionRules.stream()
                      .filter(rule -> rule.getId().equals(id))
                      .findFirst());
    }
    return super.getData(context, id)
        .or(
            () ->
                defaultDetectionExclusionRules.stream()
                    .filter(rule -> rule.getId().equals(id))
                    .findFirst());
  }

  @Override
  public List<DetectionExclusionRule> getAllConfigData(RequestContext context) {
    List<DetectionExclusionRule> detectionExclusionRules = super.getAllConfigData(context);
    if (featureFlagServiceClient.isApiProtectConfigPoliciesRevampEnabled(context)) {
      return mergeDetectionExclusionRules(
          context, detectionExclusionRules, defaultNewDetectionExclusionRules);
    }
    return mergeDetectionExclusionRules(
        context, detectionExclusionRules, defaultDetectionExclusionRules);
  }

  public List<DetectionExclusionRule> getAllConfigDataWithoutDefaults(RequestContext context) {
    return super.getAllConfigData(context);
  }

  @Override
  public List<DetectionExclusionRule> getAllConfigData(
      RequestContext context, GetRulesFilter filter) {
    List<DetectionExclusionRule> filteredDefaultDetectionExclusionRules;
    if (featureFlagServiceClient.isApiProtectConfigPoliciesRevampEnabled(context)) {
      filteredDefaultDetectionExclusionRules =
          defaultNewDetectionExclusionRules.stream()
              .filter(rule -> filterConfigData(rule, filter).isPresent())
              .collect(Collectors.toUnmodifiableList());
    } else {
      filteredDefaultDetectionExclusionRules =
          defaultDetectionExclusionRules.stream()
              .filter(rule -> filterConfigData(rule, filter).isPresent())
              .collect(Collectors.toUnmodifiableList());
    }

    List<DetectionExclusionRule> detectionExclusionRules = super.getAllConfigData(context, filter);
    return mergeDetectionExclusionRules(
        context, detectionExclusionRules, filteredDefaultDetectionExclusionRules);
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

  private List<DetectionExclusionRule> mergeDetectionExclusionRules(
      RequestContext context,
      List<DetectionExclusionRule> detectionExclusionRules,
      List<DetectionExclusionRule> defaultDetectionExclusionRules) {

    if (!SSRF_FIXED_TENANTS.contains(context.buildInternalContextualKey())) {
      // Bugfix: https://traceableai.atlassian.net/browse/ENG-33151
      detectionExclusionRules = fixSsrfRulesIfAny(context, detectionExclusionRules);
    }

    Map<String, DetectionExclusionRule> detectionExclusionRuleMap = new HashMap<>();
    detectionExclusionRuleMap.putAll(this.getRuleIdToRuleMap(defaultDetectionExclusionRules));
    detectionExclusionRuleMap.putAll(this.getRuleIdToRuleMap(detectionExclusionRules));

    return detectionExclusionRuleMap.values().stream().collect(Collectors.toUnmodifiableList());
  }

  private Map<String, DetectionExclusionRule> getRuleIdToRuleMap(
      List<DetectionExclusionRule> detectionExclusionRules) {
    return detectionExclusionRules.stream()
        .collect(Collectors.toUnmodifiableMap(DetectionExclusionRule::getId, Function.identity()));
  }

  private List<DetectionExclusionRule> fixSsrfRulesIfAny(
      RequestContext context, List<DetectionExclusionRule> detectionExclusionRules) {
    Map<String, DetectionExclusionRule> ssrfFixedRules =
        detectionExclusionRules.stream()
            .filter(
                rule ->
                    rule.getRuleInfo().getConditionsList().stream()
                        .anyMatch(
                            condition ->
                                condition.getEventCondition().getSystemDefinedEventsList().stream()
                                    .anyMatch(
                                        event ->
                                            SSRF_BAD_REPUTATION_HOST.equals(
                                                event.getEventTypeId()))))
            .map(
                rule -> {
                  List<DetectionExclusionCondition> conditions =
                      rule.getRuleInfo().getConditionsList().stream()
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
                  return rule.toBuilder()
                      .setRuleInfo(
                          rule.getRuleInfo().toBuilder()
                              .clearConditions()
                              .addAllConditions(conditions))
                      .build();
                })
            .collect(Collectors.toMap(DetectionExclusionRule::getId, Function.identity()));

    // This is going to be the majority of the case..
    if (ssrfFixedRules.isEmpty()) {
      return detectionExclusionRules;
    }

    LOGGER.info(
        "For tenant:{} fixed 'ssrf' ruleId in exclusion rules [{}]",
        context.getTenantId().get(),
        String.join(",", ssrfFixedRules.keySet()));

    try {
      // upsert the fixed rules
      upsertObjects(context, new ArrayList<>(ssrfFixedRules.values()));
      SSRF_FIXED_TENANTS.add(context.buildInternalContextualKey());
    } catch (Exception e) {
      LOGGER.error(
          "Error in upserting ssrf-fixed exclusion rules for tenant:{},",
          context.getTenantId().get(),
          e);
    }

    return detectionExclusionRules.stream()
        .map(rule -> ssrfFixedRules.getOrDefault(rule.getId(), rule))
        .collect(Collectors.toList());
  }
}
