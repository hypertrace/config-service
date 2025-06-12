package ai.traceable.customsignature.config.service.migration;

import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import jakarta.inject.Inject;
import java.util.List;

/**
 * Manager class for handling all migration operations related to custom signatures. This class
 * serves as a central point for all migration operations, making it easier to add new migration
 * logic in the future.
 */
public class CustomSignatureRuleMigrationManager {
  private final CustomSignatureRuleEvaluationPointsMigrator
      customSignatureRuleEvaluationPointsMigrator;

  @Inject
  public CustomSignatureRuleMigrationManager(
      CustomSignatureRuleEvaluationPointsMigrator customSignatureRuleEvaluationPointsMigrator) {
    this.customSignatureRuleEvaluationPointsMigrator = customSignatureRuleEvaluationPointsMigrator;
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

  public List<CustomSignatureRule> migrateRules(List<CustomSignatureRule> existingRules) {
    return customSignatureRuleEvaluationPointsMigrator.migrateRules(existingRules);
  }
}
