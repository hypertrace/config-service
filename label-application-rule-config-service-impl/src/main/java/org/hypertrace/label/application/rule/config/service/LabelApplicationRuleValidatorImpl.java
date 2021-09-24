package org.hypertrace.label.application.rule.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import com.google.protobuf.Message;
import io.grpc.Status;
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
    validateRequestContextOrThrow(requestContext);
    validateLabelApplicationRuleData(createLabelApplicationRuleRequest.getData());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      GetLabelApplicationRuleRequest getLabelApplicationRuleRequest) {
    validateRequestContextOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      GetLabelApplicationRulesRequest getLabelApplicationRulesRequest) {
    validateRequestContextOrThrow(requestContext);
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      UpdateLabelApplicationRuleRequest updateLabelApplicationRulesRequest) {
    validateRequestContextOrThrow(requestContext);
    validateLabelApplicationRuleData(updateLabelApplicationRulesRequest.getData());
  }

  @Override
  public void validateOrThrow(
      RequestContext requestContext,
      DeleteLabelApplicationRuleRequest deleteLabelApplicationRuleRequest) {
    validateRequestContextOrThrow(requestContext);
  }

  private void validateLabelApplicationRuleData(LabelApplicationRuleData labelApplicationRuleData) {
    if (!labelApplicationRuleData.hasMatchingCondition()) {
      throwInvalidArgumentException("Missing Matching Condition in request");
    }
    validateCondition(labelApplicationRuleData.getMatchingCondition());
    if (!labelApplicationRuleData.hasLabelAction()) {
      throwInvalidArgumentException("Missing Label Action in request");
    }
    validateAction(labelApplicationRuleData.getLabelAction());
  }

  private void validateCondition(LabelApplicationRuleData.Condition condition) {
    switch (condition.getConditionCase()) {
      case LEAF_CONDITION:
        validateLeafCondition(condition.getLeafCondition());
        break;
      case COMPOSITE_CONDITION:
        validateCompositeCondition(condition.getCompositeCondition());
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Unexpected Case in %s:%n %s", getName(condition), printMessage(condition)));
    }
  }

  private void validateLeafCondition(LeafCondition leafCondition) {
    if (!leafCondition.hasKeyCondition()) {
      throwInvalidArgumentException(
          String.format("Missing Key condition in Leaf Condition %s", printMessage(leafCondition)));
    }
    validateStringCondition(leafCondition.getKeyCondition());
    switch (leafCondition.getConditionCase()) {
      case STRING_CONDITION:
        validateStringCondition(leafCondition.getStringCondition());
        break;
      case UNARY_CONDITION:
        validateUnaryCondition(leafCondition.getUnaryCondition());
        break;
      case JSON_CONDITION:
        validateJsonCondition(leafCondition.getJsonCondition());
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Unexpected Case in %s:%n %s",
                getName(leafCondition), printMessage(leafCondition)));
    }
  }

  private void validateCompositeCondition(CompositeCondition compositeCondition) {
    if (compositeCondition.getOperator()
        == CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_UNSPECIFIED) {
      throwInvalidArgumentException(
          String.format(
              "Invalid Operator value in %s:%n %s",
              getName(compositeCondition), printMessage(compositeCondition)));
    }
    compositeCondition.getChildrenList().forEach(this::validateCondition);
  }

  private void validateJsonCondition(JsonCondition jsonCondition) {
    validateNonDefaultPresenceOrThrow(jsonCondition, jsonCondition.JSON_PATH_FIELD_NUMBER);
    switch (jsonCondition.getValueConditionCase()) {
      case STRING_CONDITION:
        validateStringCondition(jsonCondition.getStringCondition());
        break;
      case UNARY_CONDITION:
        validateUnaryCondition(jsonCondition.getUnaryCondition());
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Unexpected Case in %s:%n %s",
                getName(jsonCondition), printMessage(jsonCondition)));
    }
  }

  private void validateStringCondition(StringCondition stringCondition) {
    if (stringCondition.getOperator() == StringCondition.Operator.OPERATOR_UNSPECIFIED) {
      throwInvalidArgumentException(
          String.format(
              "Invalid Operator value in %s:%n %s",
              getName(stringCondition), printMessage(stringCondition)));
    }
  }

  private void validateUnaryCondition(UnaryCondition unaryCondition) {
    if (unaryCondition.getOperator() == UnaryCondition.Operator.OPERATOR_UNSPECIFIED) {
      throwInvalidArgumentException(
          String.format(
              "Invalid Operator value in %s:%n %s",
              getName(unaryCondition), printMessage(unaryCondition)));
    }
  }

  private void validateAction(Action action) {
    switch (action.getValueCase()) {
      case STATIC_LABEL_ID:
        validateNonDefaultPresenceOrThrow(action, action.STATIC_LABEL_ID_FIELD_NUMBER);
        break;
      case DYNAMIC_LABEL:
        validateDynamicLabel(action.getDynamicLabel());
        break;
      default:
        throwInvalidArgumentException(
            String.format("Unexpected Case in %s:%n %s", getName(action), printMessage(action)));
    }
    if (action.getOperation() == Action.Operation.OPERATION_UNSPECIFIED) {
      throwInvalidArgumentException(
          String.format(
              "Invalid Operator value in %s:%n %s", getName(action), printMessage(action)));
    }
  }

  private void validateDynamicLabel(Action.DynamicLabel dynamicLabel) {
    validateNonDefaultPresenceOrThrow(dynamicLabel, dynamicLabel.LABEL_EXPRESSION_FIELD_NUMBER);
    dynamicLabel.getTokenExtractionRulesList().forEach(this::validateTokenExtractionRule);
  }

  private void validateTokenExtractionRule(
      Action.DynamicLabel.TokenExtractionRule tokenExtractionRule) {
    validateNonDefaultPresenceOrThrow(tokenExtractionRule, tokenExtractionRule.KEY_FIELD_NUMBER);
  }

  private void throwInvalidArgumentException(String description) {
    throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
  }

  private String getName(Message message) {
    return message.getDescriptorForType().getName();
  }
}
