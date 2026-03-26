package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALLOW;
import static ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT;
import static ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM;

import ai.traceable.anomaly.config.service.exclusion.handlers.AnomalyExclusionRuleConfigStore;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.BulkUpsertDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionMigrationConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesStore;
import ai.traceable.platform.actor.v1.Actor;
import ai.traceable.platform.config.provider.common.clients.ActorServiceClient;
import com.google.common.collect.Sets;
import jakarta.inject.Inject;
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

  private final FeatureCachingClient featureCachingClient;
  private final DetectionExclusionRulesStore newRulesStore;
  private final AnomalyExclusionRuleConfigStore oldRulesStore;
  private final DetectionExclusionMigrationStore migrationStore;
  private final DetectionExclusionRuleConverter ruleConverter;
  private final ActorServiceClient actorServiceClient;
  private final DetectionExclusionConfigServiceConfig config;
  private final DetectionExclusionRuleEvaluationPointsMigrator ruleEvaluationPointsMigrator;

  private final Set<ContextualKey<Void>> migrationCompletedTenantsSet = new HashSet<>();
  private final Set<ContextualKey<Void>> changeLog2MigrationCompletedTenantsSet = new HashSet<>();
  private final Set<ContextualKey<Void>> changeLog3MigrationCompletedTenantsSet = new HashSet<>();
  private final Set<ContextualKey<Void>> changeLog4MigrationCompletedTenantsSet = new HashSet<>();
  private final Set<ContextualKey<Void>> ruleEvaluationPointsMigrationCompletedTenantsSet =
      new HashSet<>();
  private final Set<ContextualKey<Void>> apiProtectionExclusionRulesMigrationCompletedTenantsSet =
      new HashSet<>();
  private final Set<ContextualKey<Void>> allowOnlyPlatformRemovalMigrationCompletedTenantsSet =
      new HashSet<>();
  private final Set<ContextualKey<Void>> exclusionTargetAnyMatchFixMigrationCompletedTenantsSet =
      new HashSet<>();

  @Inject
  public DetectionExclusionRulesMigrationManager(
      FeatureCachingClient featureCachingClient,
      DetectionExclusionRulesStore newRulesStore,
      AnomalyExclusionRuleConfigStore oldRulesStore,
      DetectionExclusionMigrationStore migrationStore,
      DetectionExclusionRuleConverter ruleConverter,
      ActorServiceClient actorServiceClient,
      DetectionExclusionConfigServiceConfig config,
      DetectionExclusionRuleEvaluationPointsMigrator ruleEvaluationPointsMigrator) {
    this.featureCachingClient = featureCachingClient;
    this.newRulesStore = newRulesStore;
    this.oldRulesStore = oldRulesStore;
    this.migrationStore = migrationStore;
    this.ruleConverter = ruleConverter;
    this.actorServiceClient = actorServiceClient;
    this.config = config;
    this.ruleEvaluationPointsMigrator = ruleEvaluationPointsMigrator;
  }

  @Override
  public void migrateFromOldStoreIfApplicable(RequestContext requestContext) {
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
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
    // and need to be conditionally overridden if the latest update is through the old api
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
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
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
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
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
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
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

  @Override
  public CreateDetectionExclusionRuleRequest migrateCreateDetectionExclusionRuleRequest(
      CreateDetectionExclusionRuleRequest request) {
    return ruleEvaluationPointsMigrator.migrateCreateDetectionExclusionRuleRequest(request);
  }

  @Override
  public UpdateDetectionExclusionRuleRequest migrateUpdateDetectionExclusionRuleRequest(
      UpdateDetectionExclusionRuleRequest request) {
    return ruleEvaluationPointsMigrator.migrateUpdateDetectionExclusionRuleRequest(request);
  }

  @Override
  public BulkUpsertDetectionExclusionRulesRequest migrateBulkUpsertDetectionExclusionRulesRequest(
      BulkUpsertDetectionExclusionRulesRequest request) {
    return ruleEvaluationPointsMigrator.migrateBulkUpsertDetectionExclusionRulesRequest(request);
  }

  @Override
  public void migrateForRuleEvaluationPointsIfApplicable(RequestContext requestContext) {
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
    if (config.isRuleEvaluationPointsMigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (ruleEvaluationPointsMigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    DetectionExclusionMigrationConfig detectionExclusionMigrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(DetectionExclusionMigrationConfig.getDefaultInstance());
    if (detectionExclusionMigrationConfig.getRuleEvaluationPointsMigrationCompleted()) {
      ruleEvaluationPointsMigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateDetectionExclusionRulesWithRuleEvaluationPoints(
          requestContext, detectionExclusionMigrationConfig);
    }
  }

  @Override
  public void migrateForApiProtectionExclusionRulesIfApplicable(RequestContext requestContext) {
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    boolean isFeatureFlagEnabled =
        featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(requestContext);

    // Early cache check: if FF is enabled and we've already completed migration, skip
    if (isFeatureFlagEnabled
        && apiProtectionExclusionRulesMigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }

    // Fetch migration state from store
    DetectionExclusionMigrationConfig detectionExclusionMigrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(DetectionExclusionMigrationConfig.getDefaultInstance());

    boolean isMigrationCompleted =
        detectionExclusionMigrationConfig.getApiProtectionExclusionRulesMigrationCompleted();

    // Scenario 1: FF enabled and migration completed -> add to cache and return
    if (isFeatureFlagEnabled && isMigrationCompleted) {
      apiProtectionExclusionRulesMigrationCompletedTenantsSet.add(contextualKey);
      return;
    }

    // Scenario 2: FF disabled and migration NOT completed -> nothing to rollback
    if (!isFeatureFlagEnabled && !isMigrationCompleted) {
      return;
    }

    // Scenario 3: FF enabled and migration NOT completed -> migrate forward
    if (isFeatureFlagEnabled && !isMigrationCompleted) {
      updateDetectionExclusionRulesForApiProtection(
          requestContext, detectionExclusionMigrationConfig, true);
      return;
    }

    // Scenario 4: FF disabled and migration IS completed -> migrate backward (rollback)
    if (!isFeatureFlagEnabled && isMigrationCompleted) {
      updateDetectionExclusionRulesForApiProtection(
          requestContext, detectionExclusionMigrationConfig, false);
    }
  }

  @Override
  public void migrateForAllowOnlyPlatformRemovalIfApplicable(RequestContext requestContext) {
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
    if (config.isAllowOnlyPlatformRemovalMigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (allowOnlyPlatformRemovalMigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    DetectionExclusionMigrationConfig detectionExclusionMigrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(DetectionExclusionMigrationConfig.getDefaultInstance());
    if (detectionExclusionMigrationConfig.getAllowOnlyPlatformRemovalMigrationCompleted()) {
      allowOnlyPlatformRemovalMigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateDetectionExclusionRulesWithAllowOnlyAndPlatform(
          requestContext, detectionExclusionMigrationConfig);
    }
  }

  @Override
  public void migrateForExclusionTargetAnyMatchFixIfApplicable(RequestContext requestContext) {
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
    if (config.isExclusionTargetAnyMatchFixMigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (exclusionTargetAnyMatchFixMigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    DetectionExclusionMigrationConfig detectionExclusionMigrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(DetectionExclusionMigrationConfig.getDefaultInstance());
    if (detectionExclusionMigrationConfig.getExclusionTargetAnyMatchFixMigrationCompleted()) {
      exclusionTargetAnyMatchFixMigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateDetectionExclusionRulesWithExclusionTargetAnyMatchFix(
          requestContext, detectionExclusionMigrationConfig);
    }
  }

  private void updateDetectionExclusionRulesWithRuleEvaluationPoints(
      RequestContext requestContext, DetectionExclusionMigrationConfig migrationConfig) {
    List<DetectionExclusionRule> updatedRules =
        newRulesStore.getAllConfigData(requestContext).stream()
            .filter(rule -> rule.getRuleInfo().toBuilder().getRuleEvaluationPointsList().isEmpty())
            .map(
                rule ->
                    rule.toBuilder()
                        .setRuleInfo(
                            rule.getRuleInfo().toBuilder()
                                .addAllRuleEvaluationPoints(
                                    ruleEvaluationPointsMigrator.getRuleEvaluationPoints(
                                        rule.getRuleInfo())))
                        .build())
            .collect(Collectors.toUnmodifiableList());
    if (!updatedRules.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, updatedRules);
    }
    migrationStore.upsertObject(
        requestContext,
        migrationConfig.toBuilder().setRuleEvaluationPointsMigrationCompleted(true).build());
    ruleEvaluationPointsMigrationCompletedTenantsSet.add(
        requestContext.buildInternalContextualKey());
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

  private void updateDetectionExclusionRulesForApiProtection(
      RequestContext requestContext,
      DetectionExclusionMigrationConfig migrationConfig,
      boolean isFeatureFlagEnabled) {
    List<DetectionExclusionRule> updatedRules =
        newRulesStore.getAllConfigDataWithoutDefaults(requestContext).stream()
            .map(
                rule -> {
                  DetectionExclusionRuleInfo.Builder ruleInfoBuilder =
                      rule.getRuleInfo().toBuilder();
                  if (DetectionExclusionRuleIdMigrationManager
                      .updateRuleConditionsExclusionRulesForApiProtection(
                          ruleInfoBuilder, isFeatureFlagEnabled)) {
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
        requestContext,
        migrationConfig.toBuilder()
            .setApiProtectionExclusionRulesMigrationCompleted(isFeatureFlagEnabled)
            .build());
    if (isFeatureFlagEnabled) {
      apiProtectionExclusionRulesMigrationCompletedTenantsSet.add(
          requestContext.buildInternalContextualKey());
    } else {
      apiProtectionExclusionRulesMigrationCompletedTenantsSet.remove(
          requestContext.buildInternalContextualKey());
    }
  }

  private boolean updateConditionForSSTIifAny(DetectionExclusionRuleInfo.Builder ruleInfoBuilder) {
    return DetectionExclusionRuleIdMigrationManager.updateRuleConditions(ruleInfoBuilder);
  }

  private void updateDetectionExclusionRulesWithAllowOnlyAndPlatform(
      RequestContext requestContext,
      DetectionExclusionMigrationConfig detectionExclusionMigrationConfig) {
    List<DetectionExclusionRule> updatedRules =
        newRulesStore.getAllConfigData(requestContext).stream()
            .map(
                rule -> {
                  DetectionExclusionRuleInfo migratedRuleInfo = migrateRuleInfo(rule.getRuleInfo());
                  // Only include rules that were actually migrated
                  if (migratedRuleInfo == rule.getRuleInfo()) {
                    return null;
                  }
                  return rule.toBuilder().setRuleInfo(migratedRuleInfo).build();
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!updatedRules.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, updatedRules);
    }

    migrationStore.upsertObject(
        requestContext,
        detectionExclusionMigrationConfig.toBuilder()
            .setAllowOnlyPlatformRemovalMigrationCompleted(true)
            .build());
    allowOnlyPlatformRemovalMigrationCompletedTenantsSet.add(
        requestContext.buildInternalContextualKey());
  }

  public DetectionExclusionRuleInfo migrateRuleInfo(DetectionExclusionRuleInfo ruleInfo) {
    if (shouldNotMigrate(ruleInfo)) {
      return ruleInfo;
    }
    DetectionExclusionRuleInfo.Builder ruleInfoBuilder = ruleInfo.toBuilder();
    ruleInfoBuilder.clearRuleEvaluationPoints();
    ruleInfo.getRuleEvaluationPointsList().stream()
        .filter(ruleEvaluationPoint -> ruleEvaluationPoint != RULE_EVALUATION_POINT_PLATFORM)
        .forEach(ruleInfoBuilder::addRuleEvaluationPoints);
    if (ruleInfoBuilder.getRuleEvaluationPointsList().isEmpty()) {
      ruleInfoBuilder.addRuleEvaluationPoints(RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);
    }
    return ruleInfoBuilder.build();
  }

  private boolean shouldNotMigrate(DetectionExclusionRuleInfo ruleInfo) {
    if (ruleInfo.getExclusionTargetsList().size() != 1
        || ruleInfo.getExclusionTargets(0) != EXCLUSION_TARGET_ALLOW) {
      return true;
    }
    return !ruleInfo.getRuleEvaluationPointsList().contains(RULE_EVALUATION_POINT_PLATFORM);
  }

  private void updateDetectionExclusionRulesWithExclusionTargetAnyMatchFix(
      RequestContext requestContext,
      DetectionExclusionMigrationConfig detectionExclusionMigrationConfig) {
    List<DetectionExclusionRule> updatedRules =
        newRulesStore.getAllConfigData(requestContext).stream()
            .filter(rule -> !rule.getRuleInfo().getRuleEvaluationPointsList().isEmpty())
            .map(
                rule ->
                    rule.toBuilder()
                        .setRuleInfo(
                            rule.getRuleInfo().toBuilder()
                                .clearRuleEvaluationPoints()
                                .addAllRuleEvaluationPoints(
                                    ruleEvaluationPointsMigrator.getRuleEvaluationPoints(
                                        rule.getRuleInfo())))
                        .build())
            .collect(Collectors.toUnmodifiableList());

    if (!updatedRules.isEmpty()) {
      newRulesStore.upsertObjects(requestContext, updatedRules);
    }

    migrationStore.upsertObject(
        requestContext,
        detectionExclusionMigrationConfig.toBuilder()
            .setExclusionTargetAnyMatchFixMigrationCompleted(true)
            .build());
    exclusionTargetAnyMatchFixMigrationCompletedTenantsSet.add(
        requestContext.buildInternalContextualKey());
  }
}
