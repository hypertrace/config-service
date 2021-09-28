package org.hypertrace.label.application.rule.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import com.google.protobuf.Message;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.DeleteLabelApplicationRuleRequest;
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
    validateNonDefaultPresenceOrThrow(
        labelApplicationRuleData, labelApplicationRuleData.NAME_FIELD_NUMBER);
    validateCondition(labelApplicationRuleData.getMatchingCondition());
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
    validateNonDefaultPresenceOrThrow(compositeCondition, compositeCondition.OPERATOR_FIELD_NUMBER);
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
    validateNonDefaultPresenceOrThrow(stringCondition, stringCondition.OPERATOR_FIELD_NUMBER);
  }

  private void validateUnaryCondition(UnaryCondition unaryCondition) {
    validateNonDefaultPresenceOrThrow(unaryCondition, unaryCondition.OPERATOR_FIELD_NUMBER);
  }

  private void validateAction(Action action) {
    validateNonDefaultPresenceOrThrow(action, action.ENTITY_TYPE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(action, action.OPERATION_FIELD_NUMBER);
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
  }

  private void validateDynamicLabel(Action.DynamicLabel dynamicLabel) {
    validateNonDefaultPresenceOrThrow(dynamicLabel, dynamicLabel.LABEL_EXPRESSION_FIELD_NUMBER);
    validateLabelExpression(dynamicLabel);
    dynamicLabel.getTokenExtractionRulesList().forEach(this::validateTokenExtractionRule);
  }

  public void validateLabelExpression(Action.DynamicLabel dynamicLabel) {
    String labelExpression = dynamicLabel.getLabelExpression();
    List<String> validKeys = new ArrayList<>();
    dynamicLabel
        .getTokenExtractionRulesList()
        .forEach(
            tokenExtractionRule -> {
              validKeys.add(tokenExtractionRule.getKey());
              if (tokenExtractionRule.hasAlias()) {
                validKeys.add(tokenExtractionRule.getAlias());
              }
            });
    Pattern pattern = Pattern.compile("\\{(\\\\}|[^}])*}");
    Matcher matcher = pattern.matcher(labelExpression);
    int startOffset = 0;
    while (startOffset < labelExpression.length() && matcher.find(startOffset)) {
      String match = matcher.group();
      String key = match.substring(1, match.length() - 1);
      startOffset = startOffset + matcher.end();
      if (!validKeys.contains(key)) {
        throwInvalidArgumentException("Invalid key name in label expression");
      }
    }
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
