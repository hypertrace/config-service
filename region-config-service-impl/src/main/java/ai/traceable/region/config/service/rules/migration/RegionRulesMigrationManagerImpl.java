package ai.traceable.region.config.service.rules.migration;

import ai.traceable.region.config.service.RegionConfigServiceConfig;
import ai.traceable.region.config.service.rules.RegionRulesStore;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleMigrationConfig;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RegionRulesMigrationManagerImpl implements RegionRulesMigrationManager {
  private final RegionRulesMigrationStore migrationStore;
  private final RegionRulesStore rulesStore;
  private final RegionConfigServiceConfig config;
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
    RegionRuleMigrationConfig migrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(RegionRuleMigrationConfig.getDefaultInstance());

    if (migrationConfig.getChangeLog1MigrationCompleted()) {
      changeLog1MigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateRegionRulesFromChangeLog1(requestContext, migrationConfig);
    }
  }

  private void updateRegionRulesFromChangeLog1(
      RequestContext requestContext, RegionRuleMigrationConfig migrationConfig) {
    List<RegionRule> updatedRateLimitingRules =
        rulesStore.getAllConfigData(requestContext).stream()
            .filter(RegionRule::getInternal)
            .map(this::markRuleAsHidden)
            .collect(Collectors.toUnmodifiableList());

    if (!updatedRateLimitingRules.isEmpty()) {
      rulesStore.upsertObjects(requestContext, updatedRateLimitingRules);
    }

    migrationStore.upsertObject(
        requestContext, migrationConfig.toBuilder().setChangeLog1MigrationCompleted(true).build());
    changeLog1MigrationCompletedTenantsSet.add(requestContext.buildInternalContextualKey());
  }

  private RegionRule markRuleAsHidden(RegionRule rule) {
    return rule.toBuilder().setHidden(true).build();
  }
}
