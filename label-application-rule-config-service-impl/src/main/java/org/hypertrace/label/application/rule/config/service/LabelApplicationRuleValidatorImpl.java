package org.hypertrace.label.application.rule.config.service;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.DeleteLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRulesRequest;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Action;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.CompositeCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.JsonCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.LeafCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.StringCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.UnaryCondition;
import org.hypertrace.label.application.rule.config.service.v1.UpdateLabelApplicationRuleRequest;

public class LabelApplicationRuleValidatorImpl implements LabelApplicationRuleValidator {

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      CreateLabelApplicationRuleRequest createLabelApplicationRuleRequest) {
    validateRequestContext(requestContext);
    validateLabelApplicationRuleData(createLabelApplicationRuleRequest.getData());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      GetLabelApplicationRuleRequest getLabelApplicationRuleRequest) {
    validateRequestContext(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      GetLabelApplicationRulesRequest getLabelApplicationRulesRequest) {
    validateRequestContext(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      UpdateLabelApplicationRuleRequest updateLabelApplicationRulesRequest) {
    validateRequestContext(requestContext);
    validateLabelApplicationRuleData(updateLabelApplicationRulesRequest.getData());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      DeleteLabelApplicationRuleRequest deleteLabelApplicationRuleRequest) {
    validateRequestContext(requestContext);
  }

  private void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }

  private void validateLabelApplicationRuleData(LabelApplicationRuleData labelApplicationRuleData) {
    if (!labelApplicationRuleData.hasMatchingCondition()) {
      throw new IllegalArgumentException("Missing Matching Condition in request");
    }
    validateCondition(labelApplicationRuleData.getMatchingCondition());
    if (!labelApplicationRuleData.hasLabelAction()) {
      throw new IllegalArgumentException("Missing Label Action in request");
    }
    validateAction(labelApplicationRuleData.getLabelAction());
  }

  private void validateCondition(LabelApplicationRuleData.Condition condition) {
    if (!condition.hasLeafCondition() && !condition.hasCompositeCondition()) {
      throw new IllegalArgumentException("Condition not set");
    }
    if (condition.hasLeafCondition()) {
      validateLeafCondition(condition.getLeafCondition());
    }
    if (condition.hasCompositeCondition()) {
      validateCompositeCondition(condition.getCompositeCondition());
    }
  }

  private void validateLeafCondition(LeafCondition leafCondition) {
    validateStringCondition(leafCondition.getKeyCondition());
    if (!leafCondition.hasJsonCondition()
        && !leafCondition.hasStringCondition()
        && !leafCondition.hasUnaryCondition()) {
      throw new IllegalArgumentException("Condition not set");
    }
    if (leafCondition.hasJsonCondition()) {
      validateJsonCondition(leafCondition.getJsonCondition());
    }
    if (leafCondition.hasStringCondition()) {
      validateStringCondition(leafCondition.getStringCondition());
    }
    if (leafCondition.hasUnaryCondition()) {
      validateUnaryCondition(leafCondition.getUnaryCondition());
    }
  }

  private void validateCompositeCondition(CompositeCondition compositeCondition) {
    if (compositeCondition.getOperator()
        == CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_UNSPECIFIED) {
      throw new IllegalArgumentException("Invalid Logical Operator");
    }
    compositeCondition.getChildrenList().forEach(this::validateCondition);
  }

  private void validateJsonCondition(JsonCondition jsonCondition) {
    if (!jsonCondition.hasStringCondition() && !jsonCondition.hasUnaryCondition()) {
      throw new IllegalArgumentException("Missing String or Unary condition in JSON Condition");
    }
    if (jsonCondition.hasStringCondition()) {
      validateStringCondition(jsonCondition.getStringCondition());
    }
    if (jsonCondition.hasUnaryCondition()) {
      validateUnaryCondition(jsonCondition.getUnaryCondition());
    }
  }

  private void validateStringCondition(StringCondition stringCondition) {
    if (stringCondition.getOperator() == StringCondition.Operator.OPERATOR_UNSPECIFIED) {
      throw new IllegalArgumentException("Invalid String Condition Operator");
    }
  }

  private void validateUnaryCondition(UnaryCondition unaryCondition) {
    if (unaryCondition.getOperator() == UnaryCondition.Operator.OPERATOR_UNSPECIFIED) {
      throw new IllegalArgumentException("Invalid Unary Condition Operator");
    }
  }

  private void validateAction(Action action) {
    if (!action.hasDynamicLabel() && !action.hasStaticLabelId()) {
      throw new IllegalArgumentException("Label in Action is not set");
    }
    if (action.getOperation() == Action.Operation.OPERATION_UNSPECIFIED) {
      throw new IllegalArgumentException("Invalid Operator in Action");
    }
  }
}
