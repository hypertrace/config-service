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
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Action.DynamicLabel;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Action.DynamicLabel.TokenExtractionRule;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Action.StaticLabels;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.CompositeCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Condition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.JsonCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.LeafCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.StringKeyCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.StringValueCondition;
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
        labelApplicationRuleData, LabelApplicationRuleData.NAME_FIELD_NUMBER);
    validateCondition(labelApplicationRuleData.getMatchingCondition());
    validateAction(labelApplicationRuleData.getLabelAction());
  }

  private void validateCondition(Condition condition) {
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
        validateStringValueCondition(leafCondition.getStringCondition());
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
    validateNonDefaultPresenceOrThrow(compositeCondition, CompositeCondition.OPERATOR_FIELD_NUMBER);
    compositeCondition.getChildrenList().forEach(this::validateCondition);
  }

  private void validateJsonCondition(JsonCondition jsonCondition) {
    validateNonDefaultPresenceOrThrow(jsonCondition, JsonCondition.JSON_PATH_FIELD_NUMBER);
    switch (jsonCondition.getValueConditionCase()) {
      case STRING_CONDITION:
        validateStringValueCondition(jsonCondition.getStringCondition());
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

  private void validateKeyStringCondition(StringKeyCondition stringCondition) {
    validateNonDefaultPresenceOrThrow(stringCondition, StringKeyCondition.VALUE_FIELD_NUMBER);
    switch (stringCondition.getOperator()) {
      case OPERATOR_EQUALS:
        break;
      case OPERATOR_MATCHES_REGEX:
        final String keyPattern = stringCondition.getValue();
        final Status regexValidationStatus = RegexValidator.validate(keyPattern);
        if (!regexValidationStatus.isOk()) {
          throw regexValidationStatus
              .withDescription(
                  String.format(
                      "Invalid regex for key : %s for stringCondition: %s",
                      keyPattern, printMessage(stringCondition)))
              .asRuntimeException();
        }
        break;
      default:
        throwInvalidArgumentException(
            String.format(
                "Invalid operator for key condition: %s: %s",
                getName(stringCondition), printMessage(stringCondition)));
    }
  }

  private void validateStringValueCondition(StringValueCondition stringCondition) {
    validateNonDefaultPresenceOrThrow(stringCondition, StringValueCondition.OPERATOR_FIELD_NUMBER);
    switch (stringCondition.getOperator()) {
      case OPERATOR_MATCHES_REGEX:
      case OPERATOR_NOT_MATCHES_REGEX:
        validateNonDefaultPresenceOrThrow(stringCondition, StringValueCondition.VALUE_FIELD_NUMBER);
        final String pattern = stringCondition.getValue();
        final Status regexValidationStatus = RegexValidator.validate(pattern);
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
                  String.format(
                      "Invalid IP address or CIDR in StringCondition: %s",
                      printMessage(stringCondition)));
            }
            break;
          case VALUES:
            if (stringCondition.getValues().getValuesList().stream()
                .anyMatch(s -> !isValidIpAddressOrSubnet(s))) {
              throwInvalidArgumentException(
                  String.format(
                      "Invalid list of IP addresses or CIDRs in StringCondition: %s",
                      printMessage(stringCondition)));
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
    validateNonDefaultPresenceOrThrow(unaryCondition, UnaryCondition.OPERATOR_FIELD_NUMBER);
  }

  private void validateAction(Action action) {
    validateNonDefaultPresenceOrThrow(action, Action.ENTITY_TYPES_FIELD_NUMBER);
    validateEntityTypes(action.getEntityTypesList());
    validateNonDefaultPresenceOrThrow(action, Action.OPERATION_FIELD_NUMBER);
    switch (action.getValueCase()) {
      case STATIC_LABELS:
        validateStaticLabels(action.getStaticLabels());
        break;
      case DYNAMIC_LABEL_EXPRESSION:
        validateDynamicLabel(action.getDynamicLabelExpression());
        break;
      case DYNAMIC_LABEL_KEY:
        validateNonDefaultPresenceOrThrow(action, Action.DYNAMIC_LABEL_KEY_FIELD_NUMBER);
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

  void validateStaticLabels(StaticLabels staticLabels) {
    validateNonDefaultPresenceOrThrow(staticLabels, StaticLabels.IDS_FIELD_NUMBER);
    List<String> staticLabelIds = staticLabels.getIdsList();
    if (Set.copyOf(staticLabelIds).size() != staticLabelIds.size()) {
      throwInvalidArgumentException(
          String.format("Duplicate Static Labels %s", printMessage(staticLabels)));
    }
  }

  private void validateDynamicLabel(DynamicLabel dynamicLabel) {
    validateNonDefaultPresenceOrThrow(dynamicLabel, DynamicLabel.LABEL_EXPRESSION_FIELD_NUMBER);
    validateLabelExpression(dynamicLabel);
    dynamicLabel.getTokenExtractionRulesList().forEach(this::validateTokenExtractionRule);
  }

  public void validateLabelExpression(DynamicLabel dynamicLabel) {
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

  private void validateTokenExtractionRule(TokenExtractionRule tokenExtractionRule) {
    validateNonDefaultPresenceOrThrow(tokenExtractionRule, TokenExtractionRule.KEY_FIELD_NUMBER);
  }

  private void throwInvalidArgumentException(String description) {
    throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
  }

  private String getName(Message message) {
    return message.getDescriptorForType().getName();
  }
}
