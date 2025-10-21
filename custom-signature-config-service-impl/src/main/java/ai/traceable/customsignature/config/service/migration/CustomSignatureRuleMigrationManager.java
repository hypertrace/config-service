package ai.traceable.customsignature.config.service.migration;

import ai.traceable.customsignature.config.service.CustomSignatureConfigServiceConfig;
import ai.traceable.customsignature.config.service.rules.CustomSignatureRulesStore;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureMigrationConfig;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * Manager class for handling all migration operations related to custom signatures. This class
 * serves as a central point for all migration operations, making it easier to add new migration
 * logic in the future.
 */
public class CustomSignatureRuleMigrationManager {
  private final CustomSignatureRuleEvaluationPointsMigrator
      customSignatureRuleEvaluationPointsMigrator;
  private final CustomSignatureRulesStore rulesStore;
  private final CustomSignatureMigrationConfigStore migrationConfigStore;
  private final CustomSignatureConfigServiceConfig config;
  private final Set<ContextualKey<Void>> ruleEvaluationPointsMigrationCompletedTenantsSet =
      new HashSet<>();
  private final Set<ContextualKey<Void>> ruleCategoryMigrationCompletedTenantsSet = new HashSet<>();

  @Inject
  public CustomSignatureRuleMigrationManager(
      CustomSignatureRuleEvaluationPointsMigrator customSignatureRuleEvaluationPointsMigrator,
      CustomSignatureRulesStore rulesStore,
      CustomSignatureMigrationConfigStore migrationConfigStore,
      CustomSignatureConfigServiceConfig config) {
    this.customSignatureRuleEvaluationPointsMigrator = customSignatureRuleEvaluationPointsMigrator;
    this.rulesStore = rulesStore;
    this.migrationConfigStore = migrationConfigStore;
    this.config = config;
  }

  public CreateCustomSignatureRuleRequest migrateCreateCustomSignatureRuleRequest(
      CreateCustomSignatureRuleRequest createCustomSignatureRuleRequest) {
    return customSignatureRuleEvaluationPointsMigrator.migrateCreateCustomSignatureRuleRequest(
        createCustomSignatureRuleRequest);
  }

  public UpdateCustomSignatureRuleRequest migrateUpdateCustomSignatureRuleRequest(
      UpdateCustomSignatureRuleRequest updateCustomSignatureRuleRequest) {
    return customSignatureRuleEvaluationPointsMigrator.migrateUpdateCustomSignatureRuleRequest(
        updateCustomSignatureRuleRequest);
  }

  public void migrateCustomSignatureRules(RequestContext requestContext) {
    migrateRuleEvaluationPoints(requestContext);
    migrateRuleCategory(requestContext);
  }

  private void migrateRuleEvaluationPoints(RequestContext requestContext) {
    if (config.isRuleEvaluationPointsMigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (ruleEvaluationPointsMigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    CustomSignatureMigrationConfig migrationConfig =
        migrationConfigStore
            .getData(requestContext)
            .orElse(CustomSignatureMigrationConfig.getDefaultInstance());
    if (migrationConfig.getRuleEvaluationPointsMigrationCompleted()) {
      ruleEvaluationPointsMigrationCompletedTenantsSet.add(contextualKey);
    } else {
      customSignatureRuleEvaluationPointsMigrator.migrateRules(requestContext);
      migrationConfigStore.upsertObject(
          requestContext,
          migrationConfig.toBuilder().setRuleEvaluationPointsMigrationCompleted(true).build());
      ruleEvaluationPointsMigrationCompletedTenantsSet.add(contextualKey);
    }
  }

  private void migrateRuleCategory(RequestContext requestContext) {
    if (config.isRuleCategoryMigrationDisabled()) {
      return;
    }
    ContextualKey<Void> contextualKey = requestContext.buildInternalContextualKey();
    if (ruleCategoryMigrationCompletedTenantsSet.contains(contextualKey)) {
      return;
    }
    CustomSignatureMigrationConfig migrationConfig =
        migrationConfigStore
            .getData(requestContext)
            .orElse(CustomSignatureMigrationConfig.getDefaultInstance());
    if (migrationConfig.getRuleCategoryMigrationCompleted()) {
      ruleCategoryMigrationCompletedTenantsSet.add(contextualKey);
    } else {
      migrateRuleCategoryIfApplicable(requestContext);
      migrationConfigStore.upsertObject(
          requestContext,
          migrationConfig.toBuilder().setRuleCategoryMigrationCompleted(true).build());
      ruleCategoryMigrationCompletedTenantsSet.add(contextualKey);
    }
  }

  private void migrateRuleCategoryIfApplicable(RequestContext requestContext) {
    List<CustomSignatureRule> updatedRules =
        rulesStore.getAllConfigData(requestContext).stream()
            .filter(rule -> rule.getCategory().equals(Category.CATEGORY_UNSPECIFIED))
            .map(rule -> rule.toBuilder().setCategory(Category.CATEGORY_CUSTOM_SIGNATURE).build())
            .collect(Collectors.toUnmodifiableList());
    rulesStore.upsertObjects(requestContext, updatedRules);
  }
}
