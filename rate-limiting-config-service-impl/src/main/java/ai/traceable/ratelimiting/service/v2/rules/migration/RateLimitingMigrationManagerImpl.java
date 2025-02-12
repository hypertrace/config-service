package ai.traceable.ratelimiting.service.v2.rules.migration;

import ai.traceable.ratelimiting.config.service.v2.RateLimitingMigrationConfig;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
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

  @Override
  public void migrateFromChangeLog1IfApplicable(RequestContext requestContext) {
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

  private RateLimitingRule markRuleAsHidden(RateLimitingRule rule) {
    RuleStatus.Builder ruleStatusBuilder = rule.getData().getRuleStatus().toBuilder();
    ruleStatusBuilder.setHidden(true);

    RateLimitingRuleData.Builder ruleDataBuilder =
        rule.getData().toBuilder().setRuleStatus(ruleStatusBuilder);

    return rule.toBuilder().setData(ruleDataBuilder).build();
  }
}
