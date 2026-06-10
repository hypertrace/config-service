package ai.traceable.aiapp.protection.config.service.validator;

import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleToDelete;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleType;
import ai.traceable.aiapp.protection.config.service.v1.AiAppOotbSubRuleUpdate;
import ai.traceable.aiapp.protection.config.service.v1.AiAppRuleUpdate;
import ai.traceable.aiapp.protection.config.service.v1.CreateAiAppCustomRuleRequest;
import ai.traceable.aiapp.protection.config.service.v1.DeleteAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.GetAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.RuleAction;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatusChange;
import ai.traceable.aiapp.protection.config.service.v1.UpdateAiAppRulesRequest;
import ai.traceable.aiapp.protection.config.service.v1.UpsertAiAppCustomRuleRequest;
import io.grpc.Status;

public class AiAppProtectionConfigServiceValidator {

  /** Validates GetAiAppRulesRequest */
  public Status validateGetAiAppRulesRequest(GetAiAppRulesRequest request) {
    return validateRuleScope(request.getRuleScope());
  }

  /** Validates CreateAiAppCustomRuleRequest */
  public Status validateCreateAiAppCustomRuleRequest(CreateAiAppCustomRuleRequest request) {
    if (!request.hasAiAppCustomRuleData()) {
      return Status.INVALID_ARGUMENT.withDescription("AI app custom rule data is required");
    }

    return validateAiAppCustomRuleData(request.getAiAppCustomRuleData());
  }

  /** Validates UpsertAiAppCustomRuleRequest */
  public Status validateUpsertAiAppCustomRuleRequest(UpsertAiAppCustomRuleRequest request) {
    if (!request.hasAiAppCustomRule()) {
      return Status.INVALID_ARGUMENT.withDescription("AI app custom rule is required");
    }

    AiAppCustomRule rule = request.getAiAppCustomRule();

    // Rule ID is required for upsert operations
    if (rule.getRuleId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Rule ID is required for upsert operation");
    }

    if (!rule.hasRuleData()) {
      return Status.INVALID_ARGUMENT.withDescription("AI app custom rule data is required");
    }

    return validateAiAppCustomRuleData(rule.getRuleData());
  }

  /** Validates UpdateAiAppRulesRequest */
  public Status validateUpdateAiAppRulesRequest(UpdateAiAppRulesRequest request) {
    Status status = validateRuleScope(request.getRuleScope());
    if (!status.isOk()) {
      return status;
    }
    return request.getAiAppRuleUpdatesList().stream()
        .map(this::validateAiAppRuleUpdate)
        .filter(ruleUpdateStatus -> !ruleUpdateStatus.isOk())
        .findFirst()
        .orElse(Status.OK);
  }

  /** Validates DeleteAiAppRulesRequest */
  public Status validateDeleteAiAppRulesRequest(DeleteAiAppRulesRequest request) {
    if (!request.hasResetToDefault() && !request.hasCustomRuleToDelete()) {
      return Status.INVALID_ARGUMENT.withDescription("Delete option must be specified");
    }

    if (request.hasResetToDefault()) {
      return Status.OK;
    } else {
      return validateCustomRuleToDelete(request.getCustomRuleToDelete());
    }
  }

  private Status validateRuleScope(RuleScope ruleScope) {
    if (!ruleScope.hasEnvironmentScope() && !ruleScope.hasTenantScope()) {
      return Status.INVALID_ARGUMENT.withDescription("Rule scope is required");
    }
    if (ruleScope.hasEnvironmentScope()
        && ruleScope.getEnvironmentScope().getEnvironmentIdsList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Environment IDs are required in scope");
    }
    return Status.OK;
  }

  private Status validateAiAppRuleUpdate(AiAppRuleUpdate ruleUpdate) {
    if (ruleUpdate.getRuleId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Rule ID is required for update operation");
    }
    if (ruleUpdate.hasRuleStatusChange()) {
      Status status = validateRuleStatusChange(ruleUpdate.getRuleStatusChange());
      if (!status.isOk()) {
        return status;
      }
    }
    return ruleUpdate.getOotbSubRuleUpdatesList().stream()
        .map(this::validateAiAppOotbSubRuleUpdates)
        .filter(status -> !status.isOk())
        .findFirst()
        .orElse(Status.OK);
  }

  private Status validateAiAppOotbSubRuleUpdates(AiAppOotbSubRuleUpdate subRuleUpdate) {
    if (subRuleUpdate.getRuleId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Sub rule ID is required for update operation");
    }
    if (!subRuleUpdate.hasInternal()
        && subRuleUpdate.getRuleAction().equals(RuleAction.RULE_ACTION_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Either internal or rule action must be specified for sub rule update");
    }
    return Status.OK;
  }

  private Status validateRuleStatusChange(RuleStatusChange ruleStatusChange) {
    if (!ruleStatusChange.hasDisabled() && !ruleStatusChange.hasInternal()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Either disabled or internal must be specified for rule status change");
    }
    return Status.OK;
  }

  /** Validates AiAppCustomRuleData */
  private Status validateAiAppCustomRuleData(AiAppCustomRuleData ruleData) {
    // Validate rule name
    if (ruleData.getRuleName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Rule name is required");
    }

    // Determine and validate rule type based on rule data content
    AiAppCustomRuleType ruleType = determineRuleType(ruleData);
    if (ruleType == AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Rule data must contain one of: PII detection, rate limiting, model governance, input explosion, or sensitive data protection configuration");
    }

    if (ruleData.hasAction()
        && ruleData.getAction().hasRedact()
        && !ruleData.hasAiSensitiveDataProtectionRuleData()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Redact action is only supported for AI Sensitive Data Protection rules");
    }

    return Status.OK;
  }

  /** Validates AiAppCustomRuleToDelete */
  private Status validateCustomRuleToDelete(AiAppCustomRuleToDelete customRuleToDelete) {
    if (customRuleToDelete.getRuleId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Rule ID is required for custom rule deletion");
    }

    AiAppCustomRuleType ruleType = customRuleToDelete.getCustomRuleType();
    if (ruleType == AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom rule type must be specified for deletion");
    }

    return Status.OK;
  }

  /** Determines the rule type based on the rule data content */
  private AiAppCustomRuleType determineRuleType(AiAppCustomRuleData ruleData) {
    if (ruleData.hasPiiDetectedInPromptRuleData()) {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_PII_DETECTED_IN_PROMPT;
    } else if (ruleData.hasAiRateLimitingRuleData()) {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_RATE_LIMITING;
    } else if (ruleData.hasModelGovernanceRuleData()) {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_MODEL_GOVERNANCE;
    } else if (ruleData.hasAiInputExplosionRuleData()) {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_INPUT_EXPLOSION;
    } else if (ruleData.hasAiSensitiveDataProtectionRuleData()) {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_SENSITIVE_DATA_PROTECTION;
    } else {
      return AiAppCustomRuleType.AI_APP_CUSTOM_RULE_TYPE_UNSPECIFIED;
    }
  }
}
