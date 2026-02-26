package ai.traceable.malicioussources.config.service.rules.migration;

import ai.traceable.malicioussources.config.service.rules.MaliciousSourcesConfigServiceConfig;
import ai.traceable.malicioussources.config.service.rules.MaliciousSourcesRulesStore;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesMigrationConfig;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class MaliciousSourcesMigrationManagerImpl implements MaliciousSourcesMigrationManager {
  private final MaliciousSourcesMigrationStore migrationStore;
  private final MaliciousSourcesRulesStore rulesStore;
  private final MaliciousSourcesConfigServiceConfig config;
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
    MaliciousSourcesMigrationConfig migrationConfig =
        migrationStore
            .getData(requestContext)
            .orElse(MaliciousSourcesMigrationConfig.getDefaultInstance());

    if (migrationConfig.getChangeLog1MigrationCompleted()) {
      changeLog1MigrationCompletedTenantsSet.add(contextualKey);
    } else {
      updateMaliciousSourcesRulesFromChangeLog1(requestContext, migrationConfig);
    }
  }

  private void updateMaliciousSourcesRulesFromChangeLog1(
      RequestContext requestContext, MaliciousSourcesMigrationConfig migrationConfig) {
    List<MaliciousSourcesRule> updatedRateLimitingRules =
        rulesStore.getAllConfigData(requestContext).stream()
            .filter(rule -> rule.getRuleStatus().getInternal())
            .map(this::markRuleAsHidden)
            .collect(Collectors.toUnmodifiableList());

    if (!updatedRateLimitingRules.isEmpty()) {
      rulesStore.upsertObjects(requestContext, updatedRateLimitingRules);
    }

    migrationStore.upsertObject(
        requestContext, migrationConfig.toBuilder().setChangeLog1MigrationCompleted(true).build());
    changeLog1MigrationCompletedTenantsSet.add(requestContext.buildInternalContextualKey());
  }

  private MaliciousSourcesRule markRuleAsHidden(MaliciousSourcesRule rule) {
    MaliciousSourcesRule.Builder builder = rule.toBuilder();
    builder.getRuleStatusBuilder().setHidden(true);
    return builder.build();
  }
}
