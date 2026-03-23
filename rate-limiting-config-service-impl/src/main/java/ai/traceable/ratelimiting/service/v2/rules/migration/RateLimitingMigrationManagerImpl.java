package ai.traceable.ratelimiting.service.v2.rules.migration;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingMigrationConfig;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesStore;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RateLimitingMigrationManagerImpl implements RateLimitingMigrationManager {

  private final RateLimitingMigrationStore migrationStore;
  private final RateLimitingRulesStore rulesStore;
  private final RateLimitingConfigServiceConfig config;
  private final Set<ContextualKey<Void>> changeLog1MigrationCompletedTenantsSet = new HashSet<>();
  private final Set<ContextualKey<Void>> ruleEvaluationPointsMigrationCompletedTenantsSet =
      new HashSet<>();
  private final Set<ContextualKey<Void>> allowRulesPlatformExclusionMigrationCompletedTenantsSet =
      new HashSet<>();

  private final RateLimitingRuleEvaluationPointsMigrator ruleEvaluationPointsMigrator;

  @Override
  public void migrateFromChangeLog1IfApplicable(RequestContext requestContext) {
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
    if (config.isChangeLog1MigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (changeLog1MigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    RateLimitingMigrationConfig migrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(RateLimitingMigrationConfig.getDefaultInstance());

    if (migrationConfig.getChangeLog1MigrationCompleted()) {
      changeLog1MigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateRateLimitingRulesFromChangeLog1(requestContext, migrationConfig);
    }
  }

  @Override
  public CreateRateLimitingRuleRequest migrateCreateRateLimitingRuleRequest(
      CreateRateLimitingRuleRequest createRateLimitingRuleRequest) {
    return ruleEvaluationPointsMigrator.migrateCreateRateLimitingRuleRequest(
        createRateLimitingRuleRequest);
  }

  @Override
  public UpdateRateLimitingRuleRequest migrateUpdateRateLimitingRuleRequest(
      UpdateRateLimitingRuleRequest updateRateLimitingRuleRequest) {
    return ruleEvaluationPointsMigrator.migrateUpdateRateLimitingRuleRequest(
        updateRateLimitingRuleRequest);
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
    RateLimitingMigrationConfig rateLimitingMigrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(RateLimitingMigrationConfig.getDefaultInstance());
    if (rateLimitingMigrationConfig.getRuleEvaluationPointsMigrationCompleted()) {
      ruleEvaluationPointsMigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateRateLimitingRulesWithRuleEvaluationPoints(requestContext, rateLimitingMigrationConfig);
    }
  }

  @Override
  public void migrateAllowRulesPlatformExclusion(RequestContext requestContext) {
    requestContext = new RequestContext(requestContext).withUserTrackingSuppressed();
    if (config.isAllowRulesPlatformExclusionMigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (allowRulesPlatformExclusionMigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    RateLimitingMigrationConfig rateLimitingMigrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(RateLimitingMigrationConfig.getDefaultInstance());
    if (rateLimitingMigrationConfig.getAllowRulesPlatformExclusionMigrationCompleted()) {
      allowRulesPlatformExclusionMigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateRateLimitingRulesForPlatformExclusionInAllowRules(
          requestContext, rateLimitingMigrationConfig);
    }
  }

  private void updateRateLimitingRulesFromChangeLog1(
      RequestContext requestContext, RateLimitingMigrationConfig migrationConfig) {
    List<RateLimitingRule> updatedRateLimitingRules =
        rulesStore.getAllConfigData(requestContext).stream()
            .filter(rule -> rule.getData().getRuleStatus().getInternal())
            .map(this::markRuleAsHidden)
            .collect(Collectors.toUnmodifiableList());

    if (!updatedRateLimitingRules.isEmpty()) {
      rulesStore.upsertObjects(requestContext, updatedRateLimitingRules);
    }

    migrationStore.upsertObject(
        requestContext, migrationConfig.toBuilder().setChangeLog1MigrationCompleted(true).build());
    changeLog1MigrationCompletedTenantsSet.add(requestContext.buildInternalContextualKey());
  }

  private void updateRateLimitingRulesWithRuleEvaluationPoints(
      RequestContext requestContext, RateLimitingMigrationConfig rateLimitingMigrationConfig) {
    List<RateLimitingRule> updatedRateLimitingRules =
        rulesStore.getAllConfigData(requestContext).stream()
            .filter(rule -> rule.getData().getRuleEvaluationPointsList().isEmpty())
            .map(
                rule ->
                    rule.toBuilder()
                        .setData(
                            rule.getData().toBuilder()
                                .addAllRuleEvaluationPoints(
                                    ruleEvaluationPointsMigrator.getRuleEvaluationPoints(
                                        rule.getData()))
                                .build())
                        .build())
            .collect(Collectors.toUnmodifiableList());

    if (!updatedRateLimitingRules.isEmpty()) {
      rulesStore.upsertObjects(requestContext, updatedRateLimitingRules);
    }

    migrationStore.upsertObject(
        requestContext,
        rateLimitingMigrationConfig.toBuilder()
            .setRuleEvaluationPointsMigrationCompleted(true)
            .build());
    ruleEvaluationPointsMigrationCompletedTenantsSet.add(
        requestContext.buildInternalContextualKey());
  }

  private void updateRateLimitingRulesForPlatformExclusionInAllowRules(
      RequestContext requestContext, RateLimitingMigrationConfig rateLimitingMigrationConfig) {
    List<RateLimitingRule> updatedRateLimitingRules =
        rulesStore.getAllConfigData(requestContext).stream()
            .filter(
                rateLimitingRule ->
                    checkForPlatformRuleEvaluationPointExclusion(rateLimitingRule.getData()))
            .map(
                rateLimitingRule -> {
                  List<RuleEvaluationPoint> filteredRuleEvaluationPoints =
                      rateLimitingRule.getData().getRuleEvaluationPointsList().stream()
                          .filter(
                              ruleEvaluationPoint ->
                                  ruleEvaluationPoint
                                      != RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                          .collect(Collectors.toList());
                  if (filteredRuleEvaluationPoints.isEmpty()) {
                    filteredRuleEvaluationPoints.add(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);
                  }
                  return rateLimitingRule.toBuilder()
                      .setData(
                          rateLimitingRule.getData().toBuilder()
                              .clearRuleEvaluationPoints()
                              .addAllRuleEvaluationPoints(filteredRuleEvaluationPoints)
                              .build())
                      .build();
                })
            .collect(Collectors.toList());

    if (!updatedRateLimitingRules.isEmpty()) {
      rulesStore.upsertObjects(requestContext, updatedRateLimitingRules);
    }

    migrationStore.upsertObject(
        requestContext,
        rateLimitingMigrationConfig.toBuilder()
            .setAllowRulesPlatformExclusionMigrationCompleted(true)
            .build());
    allowRulesPlatformExclusionMigrationCompletedTenantsSet.add(
        requestContext.buildInternalContextualKey());
  }

  private RateLimitingRule markRuleAsHidden(RateLimitingRule rule) {
    RuleStatus.Builder ruleStatusBuilder = rule.getData().getRuleStatus().toBuilder();
    ruleStatusBuilder.setHidden(true);

    RateLimitingRuleData.Builder ruleDataBuilder =
        rule.getData().toBuilder().setRuleStatus(ruleStatusBuilder);

    return rule.toBuilder().setData(ruleDataBuilder).build();
  }

  private static boolean checkForPlatformRuleEvaluationPointExclusion(
      RateLimitingRuleData rateLimitingRuleData) {
    return rateLimitingRuleData
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
        && hasAllowAction(rateLimitingRuleData);
  }

  private static boolean hasAllowAction(RateLimitingRuleData rateLimitingRuleData) {
    return rateLimitingRuleData.getTransactionActionConfig().getAction().hasAllow()
        || rateLimitingRuleData.getThresholdActionConfigsList().stream()
            .flatMap(thresholdActionConfig -> thresholdActionConfig.getActionsList().stream())
            .anyMatch(Action::hasAllow);
  }
}
