package ai.traceable.userattribution.config.service.v2.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.config.utils.JsonPathValidator;
import ai.traceable.config.utils.RegexValidator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v2.CustomProjection;
import ai.traceable.userattribution.config.service.v2.DeleteUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v2.EnvironmentScope;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.LiteralValue;
import ai.traceable.userattribution.config.service.v2.LiteralValueProjection;
import ai.traceable.userattribution.config.service.v2.MatchCondition;
import ai.traceable.userattribution.config.service.v2.Predicate;
import ai.traceable.userattribution.config.service.v2.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v2.RootRelativeProjection;
import ai.traceable.userattribution.config.service.v2.ServiceScope;
import ai.traceable.userattribution.config.service.v2.StringList;
import ai.traceable.userattribution.config.service.v2.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v2.UrlScope;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleScope;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import ai.traceable.userattribution.config.service.v2.ValueMatchOperator;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class UserAttributionV2ConfigRequestValidator {
  private static final int REGEX_CAPTURE_GROUP_COUNT = 1;

  public void validateOrThrow(
      RequestContext requestContext, GetUserAttributionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, CreateUserAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateRuleData(request.getData());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateUserAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateUserAttributionRuleRequest.ID_FIELD_NUMBER);
    this.validateRuleData(request.getData());
  }

  public void validateOrThrow(UserAttributionRule existingRule, UserAttributionRule rule) {
    if (!existingRule.getData().getTemplate().equals(rule.getData().getTemplate())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("User Attribution rule template cannot be updated")
          .asRuntimeException();
    }
    if (!existingRule.getData().getSource().equals(rule.getData().getSource())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("User Attribution rule source cannot be updated")
          .asRuntimeException();
    }
  }

  public void validateOrThrow(List<UserAttributionRule> existingRules, UserAttributionRule rule) {
    boolean sameRuleExists =
        existingRules.stream()
            .anyMatch(
                existingRule ->
                    !existingRule.getId().equals(rule.getId())
                        && existingRule.getData().getName().equals(rule.getData().getName()));
    if (sameRuleExists) {
      throw Status.ALREADY_EXISTS
          .withDescription(
              String.format(
                  "User Attribution rule with name %s already exists", rule.getData().getName()))
          .asRuntimeException();
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteUserAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteUserAttributionRuleRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(
      RequestContext requestContext, RankUserAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, RankUserAttributionRuleRequest.ID_TO_UPDATE_FIELD_NUMBER);
    if (request.getIdToUpdate().equals(request.getPrecedingRuleId())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Can't rerank a rule against itself: " + printMessage(request))
          .asRuntimeException();
    }
  }

  private void validateRuleData(UserAttributionRuleData ruleData) {
    validateNonDefaultPresenceOrThrow(ruleData, UserAttributionRuleData.NAME_FIELD_NUMBER);
    this.validateRuleScope(ruleData.getScope());
    validateNonDefaultPresenceOrThrow(ruleData, UserAttributionRuleData.TEMPLATE_FIELD_NUMBER);
    final boolean hasRootTokenRule = ruleData.hasRootTokenRule();
    if (hasRootTokenRule) {
      validateUserAttributionRootTokenRule(ruleData.getRootTokenRule());
    }
    if (ruleData.hasUserIdRule()) {
      validateUserAttributionTokenRule(ruleData.getUserIdRule(), hasRootTokenRule);
    }
    if (ruleData.hasUserRoleRule()) {
      validateUserAttributionTokenRule(ruleData.getUserRoleRule(), hasRootTokenRule);
    }
    if (ruleData.hasUserScopeRule()) {
      validateUserAttributionTokenRule(ruleData.getUserRoleRule(), hasRootTokenRule);
    }
    if (ruleData.hasAuthTypeRule()) {
      validateUserAttributionTokenRule(ruleData.getAuthTypeRule(), hasRootTokenRule);
    }
    Map<String, UserAttributionTokenRule> customTokenRulesMap = ruleData.getCustomTokenRulesMap();
    customTokenRulesMap.forEach(
        (key, value) -> {
          if (key.isBlank()) {
            throw Status.INVALID_ARGUMENT
                .withDescription("Custom attribute should have a valid token name")
                .asRuntimeException();
          }
          validateUserAttributionTokenRule(value, hasRootTokenRule);
        });
  }

  private void validateRuleScope(UserAttributionRuleScope scope) {
    if (scope.hasEnvironmentScope()) {
      validateEnvironmentScope(scope.getEnvironmentScope());
    }
    if (scope.hasServiceScope()) {
      validateServiceScope(scope.getServiceScope());
    }
    if (scope.hasUrlScope()) {
      validateUrlScope(scope.getUrlScope());
    }
  }

  private void validateEnvironmentScope(EnvironmentScope scope) {
    switch (scope.getEnvironmentScopeCase()) {
      case ENVIRONMENT_NAMES:
        validateNonDefaultPresenceOrThrow(
            scope.getEnvironmentNames(), StringList.VALUES_FIELD_NUMBER);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Environment scope is not specified correctly")
            .asRuntimeException();
    }
  }

  private void validateServiceScope(ServiceScope scope) {
    switch (scope.getServiceScopeCase()) {
      case SERVICE_NAMES:
        validateNonDefaultPresenceOrThrow(scope.getServiceNames(), StringList.VALUES_FIELD_NUMBER);
        break;
      case SERVICE_NAME_REGEXES:
        validateNonDefaultPresenceOrThrow(
            scope.getServiceNameRegexes(), StringList.VALUES_FIELD_NUMBER);
        scope.getServiceNameRegexes().getValuesList().forEach(this::validatePattern);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Service scope is not specified correctly")
            .asRuntimeException();
    }
  }

  private void validateUrlScope(UrlScope scope) {
    switch (scope.getUrlScopeCase()) {
      case URL_MATCH_REGEXES:
        validateNonDefaultPresenceOrThrow(
            scope.getUrlMatchRegexes(), StringList.VALUES_FIELD_NUMBER);
        scope.getUrlMatchRegexes().getValuesList().forEach(this::validatePattern);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Url scope is not specified correctly")
            .asRuntimeException();
    }
  }

  private void validateUserAttributionRootTokenRule(UserAttributionRootTokenRule rootTokenRule) {
    if (rootTokenRule.hasTokenConditionalPredicate()) {
      validateTokenConditionalPredicate(rootTokenRule.getTokenConditionalPredicate());
    }
    validateAttributeProjection(rootTokenRule.getAttributeProjection());
  }

  private void validateUserAttributionTokenRule(
      UserAttributionTokenRule tokenRule, boolean hasRootTokenRule) {
    if (hasRootTokenRule) {
      if (tokenRule.hasAttributeProjection()) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "User attribution rule with root token rule cannot have attribute projection at leaf")
            .asRuntimeException();
      }
      if (tokenRule.hasCustomProjection()) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "User attribution rule with root token rule cannot have custom projection at leaf")
            .asRuntimeException();
      }
    }
    if (tokenRule.hasTokenConditionalPredicate()) {
      validateTokenConditionalPredicate(tokenRule.getTokenConditionalPredicate());
    }
    switch (tokenRule.getProjectionCase()) {
      case ATTRIBUTE_PROJECTION:
        validateAttributeProjection(tokenRule.getAttributeProjection());
        break;
      case CUSTOM_PROJECTION:
        validateCustomProjection(tokenRule.getCustomProjection());
        break;
      case LITERAL_VALUE_PROJECTION:
        validateLiteralValueProjection(tokenRule.getLiteralValueProjection());
        break;
      case ROOT_RELATIVE_PROJECTION:
        if (hasRootTokenRule) {
          validateRootRelativeProjection(tokenRule.getRootRelativeProjection());
        } else {
          throw Status.INVALID_ARGUMENT
              .withDescription("Root relative projection requires a root projection")
              .asRuntimeException();
        }
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Projection case is not specified correctly")
            .asRuntimeException();
    }
  }

  private void validateTokenConditionalPredicate(Predicate predicate) {
    switch (predicate.getPredicateCase()) {
      case LOGICAL_PREDICATE:
        validateLogicalPredicate(predicate.getLogicalPredicate());
        break;
      case ATTRIBUTE_PREDICATE:
        validateAttributePredicate(predicate.getAttributePredicate());
        break;
      case PREDICATE_NOT_SET:
        throw Status.INVALID_ARGUMENT.withDescription("Predicate is not set").asRuntimeException();
    }
  }

  private void validateAttributePredicate(Predicate.AttributePredicate predicate) {
    validateAttributeProjection(predicate.getAttributeProjection());
    validateAttributeMatchCondition(predicate.getAttributeValueMatchCondition());
  }

  private void validateLogicalPredicate(Predicate.LogicalPredicate predicate) {
    if (predicate.getOperator().equals(Predicate.LogicalOperator.LOGICAL_OPERATOR_UNSPECIFIED)
        || predicate.getOperator().equals(Predicate.LogicalOperator.UNRECOGNIZED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Unexpected logical operator %s", predicate.getOperator()))
          .asRuntimeException();
    }
    if (predicate.getChildrenList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Predicate children list should not be empty")
          .asRuntimeException();
    }
    for (Predicate childPredicate : predicate.getChildrenList()) {
      validateTokenConditionalPredicate(childPredicate);
    }
  }

  private void validateAttributeProjection(AttributeProjection attributeProjection) {
    validateAttribute(attributeProjection.getAttribute());
    validateValueProjections(attributeProjection.getValueProjectionsList());
  }

  private void validateCustomProjection(CustomProjection customProjection) {
    AttributeRule.Builder attributeRuleBuilder = AttributeRule.newBuilder();
    try {
      JsonFormat.parser().merge(customProjection.getCustomJson(), attributeRuleBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT.withDescription(
                  String.format("Invalid custom projection: %s", customProjection.getCustomJson())))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  private void validateLiteralValueProjection(LiteralValueProjection literalValueProjection) {
    LiteralValue literalValue = literalValueProjection.getLiteralValue();
    switch (literalValue.getValueCase()) {
      case STRING_VALUE:
        validateNonDefaultPresenceOrThrow(literalValue, LiteralValue.STRING_VALUE_FIELD_NUMBER);
        break;
      case VALUE_NOT_SET:
        throw Status.INVALID_ARGUMENT
            .withDescription("Literal value projection should have valid literal value")
            .asRuntimeException();
    }
  }

  private void validateRootRelativeProjection(RootRelativeProjection rootRelativeProjection) {
    validateValueProjections(rootRelativeProjection.getValueProjectionsList());
  }

  private void validateAttribute(Attribute attribute) {
    switch (attribute.getAttributeCase()) {
      case REQUEST_HEADER:
        validateKeyMatch(attribute.getRequestHeader());
        break;
      case REQUEST_COOKIE:
        validateKeyMatch(attribute.getRequestCookie());
        break;
      case REQUEST_QUERY_PARAMETER:
        validateKeyMatch(attribute.getRequestQueryParameter());
        break;
      case RESPONSE_HEADER:
        validateKeyMatch(attribute.getResponseHeader());
        break;
      case RESPONSE_COOKIE:
        validateKeyMatch(attribute.getResponseCookie());
        break;
      case SPAN_ATTRIBUTE:
        validateKeyMatch(attribute.getSpanAttribute());
        break;
      case ATTRIBUTE_NOT_SET:
        throw Status.INVALID_ARGUMENT
            .withDescription("Attribute is not set correctly")
            .asRuntimeException();
    }
  }

  private void validateKeyMatch(KeyMatch keyMatch) {
    validateNonDefaultPresenceOrThrow(keyMatch, KeyMatch.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(keyMatch, KeyMatch.MATCH_KEY_FIELD_NUMBER);
  }

  private void validateAttributeMatchCondition(MatchCondition matchCondition) {
    validateNonDefaultPresenceOrThrow(matchCondition, MatchCondition.OPERATOR_FIELD_NUMBER);
    ValueMatchOperator operator = matchCondition.getOperator();
    if (matchCondition.getMatchValue().getValueCase() == LiteralValue.ValueCase.NULL_VALUE) {
      if (!(operator == ValueMatchOperator.VALUE_MATCH_OPERATOR_NOT_EQUALS
          || operator == ValueMatchOperator.VALUE_MATCH_OPERATOR_EQUALS)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Match operator must be equal or not equal for null value: %s",
                    printMessage(matchCondition.getMatchValue())))
            .asRuntimeException();
      }
      return;
    }
    validateNonDefaultPresenceOrThrow(
        matchCondition.getMatchValue(), LiteralValue.STRING_VALUE_FIELD_NUMBER);
    if (operator == ValueMatchOperator.VALUE_MATCH_OPERATOR_MATCHES_REGEX) {
      Status status = RegexValidator.validateRegex(matchCondition.getMatchValue().getStringValue());
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
    }
  }

  private void validateValueProjections(List<ValueProjection> projections) {
    projections.forEach(
        projection -> {
          switch (projection.getProjectionCase()) {
            case PROJECTION_NOT_SET:
              throw Status.INVALID_ARGUMENT
                  .withDescription(
                      String.format("Unexpected value projection: %s", printMessage(projection)))
                  .asRuntimeException();
            case REGEX_CAPTURE_GROUP:
              Status regexValidatorStatus =
                  RegexValidator.validateCaptureGroupCount(
                      projection.getRegexCaptureGroup().getRegex(), REGEX_CAPTURE_GROUP_COUNT);
              if (!regexValidatorStatus.isOk()) {
                throw regexValidatorStatus.asRuntimeException();
              }
              break;
            case JSON_PATH:
              validateNonDefaultPresenceOrThrow(
                  projection.getJsonPath(), ValueProjection.JsonPathProjection.PATH_FIELD_NUMBER);
              Status jsonPathValidatorStatus =
                  JsonPathValidator.validate(projection.getJsonPath().getPath());
              if (!jsonPathValidatorStatus.isOk()) {
                throw jsonPathValidatorStatus.asRuntimeException();
              }
              break;
            case JWT_PAYLOAD_CLAIM:
              validateNonDefaultPresenceOrThrow(
                  projection.getJwtPayloadClaim(),
                  ValueProjection.JwtPayloadClaimProjection.CLAIM_KEY_FIELD_NUMBER);
          }
        });
  }

  private void validatePattern(String regex) {
    Status status = RegexValidator.validateRegex(regex);
    if (!status.isOk()) {
      throw status.asRuntimeException();
    }
  }
}
