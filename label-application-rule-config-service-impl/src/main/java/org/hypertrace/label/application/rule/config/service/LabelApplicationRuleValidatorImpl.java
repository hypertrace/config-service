package org.hypertrace.label.application.rule.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.isValidIpAddressOrSubnet;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import com.google.protobuf.Message;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.hypertrace.config.validation.RegexValidator;
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
    validateKeyStringCondition(leafCondition.getKeyCondition());
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

  private void validateKeyStringCondition(StringCondition stringCondition) {
    validateNonDefaultPresenceOrThrow(stringCondition, StringCondition.OPERATOR_FIELD_NUMBER);
    switch (stringCondition.getOperator()) {
      case OPERATOR_EQUALS:
      case OPERATOR_MATCHES_REGEX:
        return;
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid operator for key condition: %s: %s",
                getName(stringCondition), printMessage(stringCondition)));
    }
  }

  private void validateStringCondition(StringCondition stringCondition) {
    validateNonDefaultPresenceOrThrow(stringCondition, StringCondition.OPERATOR_FIELD_NUMBER);
    switch (stringCondition.getOperator()) {
      case OPERATOR_MATCHES_REGEX:
        final String pattern = stringCondition.getValue();
        Status regexValidationStatus = RegexValidator.validate(pattern);
        if (!regexValidationStatus.isOk()) {
          throw regexValidationStatus
              .withDescription(String.format("Invalid regex : %s", pattern))
              .asRuntimeException();
        }
        break;
      case OPERATOR_MATCHES_IPS:
      case OPERATOR_NOT_MATCHES_IPS:
        switch (stringCondition.getKindCase()) {
          case VALUE:
            final String ip = stringCondition.getValue();
            if (!isValidIpAddressOrSubnet(ip)) {
              throwInvalidArgumentException(
                  "StringCondition in LabelApplicationRule should have a valid IP address or CIDR");
            }
            break;
          case VALUES:
            if (stringCondition.getValues().getValuesList().stream()
                .anyMatch(s -> !isValidIpAddressOrSubnet(s))) {
              throwInvalidArgumentException(
                  "StringCondition in LabelApplicationRule should have a valid list of IP addresses and CIDRs");
            }
            break;
          default:
            throwInvalidArgumentException(
                String.format(
                    "Unexpected Case in %s:%n %s",
                    getName(stringCondition), printMessage(stringCondition)));
        }
    }
  }

  private void validateUnaryCondition(UnaryCondition unaryCondition) {
    validateNonDefaultPresenceOrThrow(unaryCondition, unaryCondition.OPERATOR_FIELD_NUMBER);
  }

  private void validateAction(Action action) {
    validateNonDefaultPresenceOrThrow(action, action.ENTITY_TYPES_FIELD_NUMBER);
    validateEntityTypes(action.getEntityTypesList());
    validateNonDefaultPresenceOrThrow(action, action.OPERATION_FIELD_NUMBER);
    switch (action.getValueCase()) {
      case STATIC_LABELS:
        validateStaticLabels(action.getStaticLabels());
        break;
      case DYNAMIC_LABEL_EXPRESSION:
        validateDynamicLabel(action.getDynamicLabelExpression());
        break;
      case DYNAMIC_LABEL_KEY:
        validateNonDefaultPresenceOrThrow(action, action.DYNAMIC_LABEL_KEY_FIELD_NUMBER);
        break;
      default:
        throwInvalidArgumentException(
            String.format("Unexpected Case in %s:%n %s", getName(action), printMessage(action)));
    }
  }

  void validateEntityTypes(List<String> entityTypes) {
    if (Set.copyOf(entityTypes).size() != entityTypes.size()) {
      throwInvalidArgumentException(String.format("Duplicate entity types %s", entityTypes));
    }
  }

  void validateStaticLabels(Action.StaticLabels staticLabels) {
    validateNonDefaultPresenceOrThrow(staticLabels, staticLabels.IDS_FIELD_NUMBER);
    List<String> staticLabelIds = staticLabels.getIdsList();
    if (Set.copyOf(staticLabelIds).size() != staticLabelIds.size()) {
      throwInvalidArgumentException(
          String.format("Duplicate Static Labels %s", printMessage(staticLabels)));
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
