package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.DeleteCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.UpdateCustomSignatureRuleRequest;
import io.grpc.Status;
import java.time.Duration;
import java.time.format.DateTimeParseException;

class CustomSignatureRulesValidator implements RulesValidator {
  @Override
  public Status validate(CreateCustomSignatureRuleRequest request) {
    if (request.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid name.");
    }

    Status status;

    if (!request.hasEffect()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid effect.");
    }
    if ((status = validateRuleEffect(request.getEffect())) != Status.OK) {
      return status;
    }

    if (!request.hasDefinition()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid definition.");
    }
    if ((status = validateRuleDefinition(request.getDefinition())) != Status.OK) {
      return status;
    }

    if ((status = validateExpiry(request.getBlockingExpiryDetails())) != Status.OK) {
      return status;
    }

    return Status.OK;
  }

  @Override
  public Status validate(UpdateCustomSignatureRuleRequest request) {
    CustomSignatureRule rule = request.getRule();
    if (rule.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Update custom signature rule should have a valid id.");
    }
    if (rule.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Update custom signature rule should have a valid name.");
    }

    Status status;

    if (!rule.hasEffect()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid effect.");
    }
    if ((status = validateRuleEffect(rule.getEffect())) != Status.OK) {
      return status;
    }

    if (!rule.hasDefinition()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule should have a valid definition.");
    }
    if ((status = validateRuleDefinition(rule.getDefinition())) != Status.OK) {
      return status;
    }

    if ((status = validateExpiry(rule.getBlockingExpiryDetails())) != Status.OK) {
      return status;
    }

    return Status.OK;
  }

  @Override
  public Status validate(DeleteCustomSignatureRuleRequest request) {
    String ruleId = request.getId();
    if (ruleId.isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Delete custom signature rule should have a valid id");
    }

    return Status.OK;
  }

  private Status validateRuleEffect(RuleEffect ruleEffect) {
    if (ruleEffect.getEventType() == EventType.EVENT_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Effect should have a valid event type.");
    }
    if (ruleEffect.getEventSeverity() == EventSeverity.EVENT_SEVERITY_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Effect should have a valid event severity.");
    }
    return Status.OK;
  }

  private Status validateRuleDefinition(RuleDefinition ruleDefinition) {
    if (!ruleDefinition.hasClauseGroup()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create custom signature rule definition should have a valid clause group.");
    }
    return validateClauseGroup(ruleDefinition.getClauseGroup());
  }

  private Status validateClauseGroup(ClauseGroup clauseGroup) {
    if (clauseGroup.getClauseOperator() == ClauseOperator.CLAUSE_OPERATOR_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Definition clause group should have a valid clause operator.");
    }
    if (clauseGroup.getClausesList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule Definition clause group should have at least one clause.");
    }
    Status status;
    for (Clause clause : clauseGroup.getClausesList()) {
      if ((status = validateClause(clause)) != Status.OK) {
        return status;
      }
    }
    return Status.OK;
  }

  private Status validateClause(Clause clause) {
    if (clause.hasMatchExpression()) {
      return validateMatchExpression(clause.getMatchExpression());
    }
    if (clause.hasKeyValueExpression()) {
      return validateKeyValueExpression(clause.getKeyValueExpression());
    }
    return Status.INVALID_ARGUMENT.withDescription(
        "Custom Signature Rule Clause should have a valid expression");
  }

  private Status validateMatchExpression(MatchExpression matchExpression) {
    if (matchExpression.getMatchKey() == MatchKey.MATCH_KEY_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule match expression should have a valid match key.");
    }
    if (matchExpression.getMatchOperator() == MatchOperator.MATCH_OPERATOR_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule match expression should have a valid match operator.");
    }
    return Status.OK;
  }

  private Status validateKeyValueExpression(KeyValueExpression keyValueExpression) {
    if (keyValueExpression.getTag() == KeyValueTag.KEY_VALUE_TAG_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule key-value expression should have a valid tag.");
    }
    if (keyValueExpression.getMatchKey().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule key-value expression should have a valid match key.");
    }
    if (keyValueExpression.getKeyMatchOperator() == MatchOperator.MATCH_OPERATOR_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule key-value expression should have a valid key match operator.");
    }
    if (keyValueExpression.getMatchValue().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule key-value expression should have a valid match value.");
    }
    if (keyValueExpression.getValueMatchOperator() == MatchOperator.MATCH_OPERATOR_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Custom Signature Rule key-value expression should have a valid value match operator.");
    }
    return Status.OK;
  }

  private static Status validateExpiry(ExpiryDetails expiry) {
    if (expiry.hasExpiryDuration()) {
      try {
        Duration.parse(expiry.getExpiryDuration());
      } catch (DateTimeParseException e) {
        return Status.INVALID_ARGUMENT.withDescription("Blocking expiry duration can't be parsed");
      }
    }
    return Status.OK;
  }
}
