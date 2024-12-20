package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.anomaly.config.service.exclusion.handlers.AnomalyExclusionRuleConfigStore;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionMigrationConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesStore;
import ai.traceable.platform.actor.v1.Actor;
import ai.traceable.platform.config.provider.common.clients.ActorServiceClient;
import com.google.common.collect.Sets;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DetectionExclusionRulesMigrationManager implements RulesMigrationManager {

  static final GetRulesFilter OLD_RULES_FILTER =
      GetRulesFilter.newBuilder().addRuleCreationSources(RuleSource.RULE_SOURCE_OLD_API).build();
  private static final String SSTI_OLD_SUB_RULE_ID = "crs_9210310";
  private static final String SSTI_NEW_SUB_RULE_ID = "crs_9320310";

  private final FeatureCachingClient featureCachingClient;
  private final DetectionExclusionRulesStore newRulesStore;
  private final AnomalyExclusionRuleConfigStore oldRulesStore;
  private final DetectionExclusionMigrationStore migrationStore;
  private final DetectionExclusionRuleConverter ruleConverter;
  private final ActorServiceClient actorServiceClient;
  private final DetectionExclusionConfigServiceConfig config;

  private final Set<ContextualKey<Void>> migrationCompletedTenantsSet = new HashSet<>();
  private final Set<ContextualKey<Void>> changeLog2MigrationCompletedTenantsSet = new HashSet<>();
  private final Set<ContextualKey<Void>> changeLog3MigrationCompletedTenantsSet = new HashSet<>();
  private final Set<ContextualKey<Void>> changeLog4MigrationCompletedTenantsSet = new HashSet<>();

  @Inject
  public DetectionExclusionRulesMigrationManager(
      FeatureCachingClient featureCachingClient,
      DetectionExclusionRulesStore newRulesStore,
      AnomalyExclusionRuleConfigStore oldRulesStore,
      DetectionExclusionMigrationStore migrationStore,
      DetectionExclusionRuleConverter ruleConverter,
      ActorServiceClient actorServiceClient,
      DetectionExclusionConfigServiceConfig config) {
    this.featureCachingClient = featureCachingClient;
    this.newRulesStore = newRulesStore;
    this.oldRulesStore = oldRulesStore;
    this.migrationStore = migrationStore;
    this.ruleConverter = ruleConverter;
    this.actorServiceClient = actorServiceClient;
    this.config = config;
  }

  @Override
  public void migrateFromOldStoreIfApplicable(RequestContext requestContext) {
    if (config.isMigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (migrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    DetectionExclusionMigrationConfig migrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(DetectionExclusionMigrationConfig.getDefaultInstance());

    if (migrationConfig.getMigrationCompleted()) {
      migrationCompletedTenantsSet.add(contextualKey);
    } else if (featureCachingClient.isDetectionExclusionV2EnabledForTenant(requestContext)) {
      updateDetectionExclusionRulesFromOldStore(requestContext, migrationConfig);
    }
  }

  private void updateDetectionExclusionRulesFromOldStore(
      RequestContext requestContext, DetectionExclusionMigrationConfig migrationConfig) {
    Map<String, ContextualConfigObject<DetectionExclusionRule>> newRuleObjects =
        newRulesStore.getAllObjects(requestContext, OLD_RULES_FILTER).stream()
            .collect(Collectors.toMap(object -> object.getData().getId(), Function.identity()));
    Map<String, ContextualConfigObject<AnomalyExclusionRuleConfig>> oldRuleObjects =
        oldRulesStore.getAllObjects(requestContext).stream()
            .collect(Collectors.toMap(object -> object.getData().getId(), Function.identity()));

    List<String> oldRulesActorEntityIds =
        oldRuleObjects.values().stream()
            .map(ConfigObject::getData)
            .filter(rule -> rule.getRuleData().getAnomalyActorExclusionInfo().hasAnomalyActor())
            .map(
                rule -> rule.getRuleData().getAnomalyActorExclusionInfo().getAnomalyActor().getId())
            .collect(Collectors.toUnmodifiableList());

    Map<String, String> oldRulesActorEntityIdToIdMap =
        actorServiceClient
            .getActorsByEntityIds(
                requestContext.getTenantId().orElseThrow(), oldRulesActorEntityIds)
            .stream()
            .collect(Collectors.toUnmodifiableMap(Actor::getEntityId, Actor::getActorId));

    // rules created by old api, not present in new api
    List<DetectionExclusionRule> oldRulesToCreate =
        Sets.difference(oldRuleObjects.keySet(), newRuleObjects.keySet()).stream()
            .map(
                key ->
                    ruleConverter.convertRule(
                        oldRuleObjects.get(key).getData(), oldRulesActorEntityIdToIdMap))
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

    // rules present in both stores
    // need to be conditionally overridden if latest update is by old api
    List<DetectionExclusionRule> oldRulesToUpdate =
        Sets.intersection(oldRuleObjects.keySet(), newRuleObjects.keySet()).stream()
            .filter(
                key ->
                    oldRuleObjects.get(key).getLastUpdatedTimestamp().toEpochMilli()
                        > newRuleObjects.get(key).getLastUpdatedTimestamp().toEpochMilli())
            .map(
                key ->
                    ruleConverter.convertRule(
                        oldRuleObjects.get(key).getData(), oldRulesActorEntityIdToIdMap))
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

    if (!oldRulesToCreate.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, oldRulesToCreate);
    }
    if (!oldRulesToUpdate.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, oldRulesToUpdate);
    }

    migrationStore.upsertObject(
        requestContext, migrationConfig.toBuilder().setMigrationCompleted(true).build());
    migrationCompletedTenantsSet.add(requestContext.buildInternalContextualKey());
  }

  @Override
  public void migrateFromChangeLog2IfApplicable(RequestContext requestContext) {
    if (config.isChangeLog2MigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (changeLog2MigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    DetectionExclusionMigrationConfig migrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(DetectionExclusionMigrationConfig.getDefaultInstance());

    if (migrationConfig.getChangeLog2MigrationCompleted()) {
      changeLog2MigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateDetectionExclusionRulesFromChangeLog2(requestContext, migrationConfig);
    }
  }

  @Override
  public void migrateFromChangeLog3IfApplicable(RequestContext requestContext) {
    if (config.isChangeLog3MigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (changeLog3MigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    DetectionExclusionMigrationConfig migrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(DetectionExclusionMigrationConfig.getDefaultInstance());

    if (migrationConfig.getChangeLog3MigrationCompleted()) {
      changeLog3MigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateDetectionExclusionRulesFromChangeLog3(requestContext, migrationConfig);
    }
  }

  @Override
  public void migrateFromChangeLog4IfApplicable(RequestContext requestContext) {
    if (config.isChangeLog4MigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (changeLog4MigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    DetectionExclusionMigrationConfig migrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(DetectionExclusionMigrationConfig.getDefaultInstance());

    if (migrationConfig.getChangeLog4MigrationCompleted()) {
      changeLog4MigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateDetectionExclusionRulesFromChangeLog4(requestContext, migrationConfig);
    }
  }

  private void updateDetectionExclusionRulesFromChangeLog2(
      RequestContext requestContext, DetectionExclusionMigrationConfig migrationConfig) {
    List<DetectionExclusionRule> updatedRules =
        newRulesStore.getAllConfigData(requestContext).stream()
            .filter(rule -> rule.getRuleInfo().toBuilder().getExclusionTargetsList().isEmpty())
            .map(
                rule ->
                    rule.toBuilder()
                        .setRuleInfo(
                            rule.getRuleInfo().toBuilder()
                                .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALERT))
                        .build())
            .collect(Collectors.toUnmodifiableList());

    if (!updatedRules.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, updatedRules);
    }
    migrationStore.upsertObject(
        requestContext, migrationConfig.toBuilder().setChangeLog2MigrationCompleted(true).build());
    changeLog2MigrationCompletedTenantsSet.add(requestContext.buildInternalContextualKey());
  }

  private void updateDetectionExclusionRulesFromChangeLog3(
      RequestContext requestContext, DetectionExclusionMigrationConfig migrationConfig) {
    List<DetectionExclusionRule> updatedRules =
        newRulesStore.getAllConfigData(requestContext).stream()
            .map(
                rule -> {
                  DetectionExclusionRuleInfo.Builder ruleInfoBuilder =
                      rule.getRuleInfo().toBuilder();
                  if (updateConditionForSSTIifAny(ruleInfoBuilder)) {
                    return rule.toBuilder().setRuleInfo(ruleInfoBuilder).build();
                  }
                  return null;
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());
    if (!updatedRules.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, updatedRules);
    }
    migrationStore.upsertObject(
        requestContext, migrationConfig.toBuilder().setChangeLog3MigrationCompleted(true).build());
    changeLog3MigrationCompletedTenantsSet.add(requestContext.buildInternalContextualKey());
  }

  private void updateDetectionExclusionRulesFromChangeLog4(
      RequestContext requestContext, DetectionExclusionMigrationConfig migrationConfig) {
    List<DetectionExclusionRule> updatedRules =
        newRulesStore.getAllConfigData(requestContext).stream()
            .map(
                rule -> {
                  if (!rule.getRuleInfo().getRuleStatus().getHidden()
                      && isApplicableToBeMarkedHidden(rule.getRuleInfo().getConditionsList())) {
                    DetectionExclusionRuleInfo.Builder builder = rule.getRuleInfo().toBuilder();
                    builder.getRuleStatusBuilder().setHidden(true);
                    return rule.toBuilder().setRuleInfo(builder).build();
                  }
                  return null;
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

    if (!updatedRules.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, updatedRules);
    }
    migrationStore.upsertObject(
        requestContext, migrationConfig.toBuilder().setChangeLog4MigrationCompleted(true).build());
    changeLog4MigrationCompletedTenantsSet.add(requestContext.buildInternalContextualKey());
  }

  private boolean isApplicableToBeMarkedHidden(List<DetectionExclusionCondition> conditions) {
    return conditions.stream()
        .anyMatch(
            condition ->
                condition.hasSourceAnomalousAttributeMatchCondition()
                    || condition.hasSourceScopeCondition());
  }

  private boolean updateConditionForSSTIifAny(DetectionExclusionRuleInfo.Builder ruleInfoBuilder) {
    boolean isUpdated = false;
    List<DetectionExclusionCondition> conditions = new ArrayList<>();
    for (DetectionExclusionCondition condition : ruleInfoBuilder.getConditionsList()) {
      if (condition.getEventCondition().getSystemDefinedEventsList().stream()
          .anyMatch(event -> SSTI_OLD_SUB_RULE_ID.equals(event.getEventSubTypeId()))) {
        List<SystemDefinedEvent> events =
            condition.getEventCondition().getSystemDefinedEventsList().stream()
                .map(
                    event -> {
                      if (SSTI_OLD_SUB_RULE_ID.equals(event.getEventSubTypeId())) {
                        return event.toBuilder().setEventSubTypeId(SSTI_NEW_SUB_RULE_ID).build();
                      }
                      return event;
                    })
                .collect(Collectors.toList());
        conditions.add(
            condition.toBuilder()
                .setEventCondition(
                    condition.getEventCondition().toBuilder()
                        .clearSystemDefinedEvents()
                        .addAllSystemDefinedEvents(events))
                .build());
        isUpdated = true;
      } else {
        conditions.add(condition);
      }
    }
    if (isUpdated) {
      ruleInfoBuilder.clearConditions();
      ruleInfoBuilder.addAllConditions(conditions);
    }
    return isUpdated;
  }
}
