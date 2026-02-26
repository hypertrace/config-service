package ai.traceable.iprange.config.service.rules.migration;

import ai.traceable.iprange.config.service.IpRangeConfigServiceConfig;
import ai.traceable.iprange.config.service.rules.IpRangeRulesStore;
import ai.traceable.iprange.config.service.v1.IpRangeMigrationConfig;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class IpRangeRulesMigrationManagerImpl implements IpRangeRulesMigrationManager {
  private final IpRangeRulesMigrationStore migrationStore;
  private final IpRangeRulesStore rulesStore;
  private final IpRangeConfigServiceConfig config;
  private final Set<ContextualKey<Void>> changeLog1MigrationCompletedTenantsSet = new HashSet<>();

  @Override
  public void migrateFromChangeLog1IfApplicable(RequestContext requestContext) {
    requestContext = requestContext.withUserTrackingSuppressed();
    if (config.isChangeLog1MigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (changeLog1MigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    IpRangeMigrationConfig migrationConfig =
        migrationStore.getData(requestContext).orElse(IpRangeMigrationConfig.getDefaultInstance());

    if (migrationConfig.getChangeLog1MigrationCompleted()) {
      changeLog1MigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateIpRangeRulesFromChangeLog1(requestContext, migrationConfig);
    }
  }

  private void updateIpRangeRulesFromChangeLog1(
      RequestContext requestContext, IpRangeMigrationConfig migrationConfig) {
    List<IpRangeRule> updatedRateLimitingRules =
        rulesStore.getAllConfigData(requestContext).stream()
            .filter(IpRangeRule::getInternal)
            .map(this::markRuleAsHidden)
            .collect(Collectors.toUnmodifiableList());

    if (!updatedRateLimitingRules.isEmpty()) {
      rulesStore.upsertObjects(requestContext, updatedRateLimitingRules);
    }

    migrationStore.upsertObject(
        requestContext, migrationConfig.toBuilder().setChangeLog1MigrationCompleted(true).build());
    changeLog1MigrationCompletedTenantsSet.add(requestContext.buildInternalContextualKey());
  }

  private IpRangeRule markRuleAsHidden(IpRangeRule rule) {
    return rule.toBuilder().setHidden(true).build();
  }
}
