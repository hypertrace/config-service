package ai.traceable.userattribution.config.service.v1.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomJsonUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomTokenRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.JwtUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ParsingTarget;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ResponseBodyUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RuleCondition;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import com.google.re2j.Pattern;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class UserAttributionConfigRequestValidator {
  private static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final Parser JSON_FORMAT_PARSER = JsonFormat.parser();
  private final AuthenticationValidator authenticationValidator;

  public void validateOrThrow(
      RequestContext requestContext, GetUserAttributionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, CreateUserAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateUserAttributionRuleRequest.NAME_FIELD_NUMBER);
    this.validateRuleData(request.getData(), request.getScope());
    this.validateRuleScope(request.getScope());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateUserAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    UserAttributionRule rule = request.getRule();
    validateNonDefaultPresenceOrThrow(rule, UserAttributionRule.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(rule, UserAttributionRule.NAME_FIELD_NUMBER);
    this.validateRuleData(rule.getData(), rule.getScope());
    this.validateRuleScope(rule.getScope());
  }

  public void validateUpdateOrThrow(UserAttributionRule existing, UserAttributionRule updated) {
    if (existing.getRank() != updated.getRank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Updated rule must match existing rule rank. Use rank API to adjust ranks")
          .asRuntimeException();
    }
  }

  public void validateOrThrow(List<UserAttributionRule> existingRules, UserAttributionRule rule) {
    boolean sameRuleExists =
        existingRules.stream()
            .anyMatch(existingRule -> existingRule.getName().equals(rule.getName()));
    if (sameRuleExists) {
      throw Status.ALREADY_EXISTS
          .withDescription(
              String.format("User Attribution rule with name %s already exists", rule.getName()))
          .asRuntimeException();
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteUserAttributionRuleRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteUserAttributionRuleRequest.RULE_ID_FIELD_NUMBER);
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

  private void validateRuleData(UserAttributionRuleData ruleData, UserAttributionRuleScope scope) {
    switch (ruleData.getDataCase()) {
      case JWT_DATA:
        this.validateJwtData(ruleData.getJwtData());
        break;
      case BASIC_AUTHENTICATION_DATA:
        // No validation required, no data expected
        break;
      case RESPONSE_BODY_DATA:
        this.validateResponseBodyData(ruleData.getResponseBodyData(), scope);
        break;
      case REQUEST_HEADER_DATA:
        this.validateRequestHeaderRuleData(ruleData.getRequestHeaderData());
        break;
      case CUSTOM_TOKEN_DATA:
        this.validateCustomTokenData(ruleData.getCustomTokenData());
        break;
      case CUSTOM_DATA:
        this.validateCustomRuleData(ruleData.getCustomData());
        break;
      case CUSTOM_JSON_DATA:
        this.validateCustomJsonRuleData(ruleData.getCustomJsonData());
        break;
      case DATA_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected data case: " + printMessage(ruleData))
            .asRuntimeException();
    }
  }

  private void validateCustomRuleData(CustomUserAttributionRuleData customRuleData) {
    validateNonDefaultPresenceOrThrow(
        customRuleData, CustomUserAttributionRuleData.YAML_FIELD_NUMBER);
    this.validateValidYaml(customRuleData.getYaml());
  }

  private void validateCustomJsonRuleData(CustomJsonUserAttributionRuleData customJsonRuleData) {
    if (!customJsonRuleData.hasUserIdRuleData()
        && !customJsonRuleData.hasRoleRuleData()
        && !customJsonRuleData.hasAuthTypeRuleData()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "JSON rule data should be specified for at least one of the target attributes")
          .asRuntimeException();
    }
    if (customJsonRuleData.hasUserIdRuleData()) {
      this.validateJsonRuleData(customJsonRuleData.getUserIdRuleData());
    }
    if (customJsonRuleData.hasRoleRuleData()) {
      this.validateJsonRuleData(customJsonRuleData.getRoleRuleData());
    }
    if (customJsonRuleData.hasAuthTypeRuleData()) {
      this.validateJsonRuleData(customJsonRuleData.getAuthTypeRuleData());
    }
  }

  private void validateRequestHeaderRuleData(
      RequestHeaderUserAttributionRuleData requestHeaderRuleData) {
    this.validateHeaderLocation(requestHeaderRuleData.getUserIdLocation());
    if (requestHeaderRuleData.hasAuthentication()) {
      authenticationValidator.validate(requestHeaderRuleData.getAuthentication());
    }
    // Role not required
  }

  private void validateResponseBodyData(
      ResponseBodyUserAttributionRuleData responseBodyRuleData, UserAttributionRuleScope scope) {
    if (responseBodyRuleData.hasCondition()
        && responseBodyRuleData.getCondition().hasUrlMatchRegex()) {
      this.validateRuleCondition(responseBodyRuleData.getCondition());
    } else if (!scope.hasCustomScope() || scope.getCustomScope().getUrlScopesCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Response body authentication type rule (%s) requires at least one URL scope",
                  printMessage(responseBodyRuleData)))
          .asRuntimeException();
    }

    this.validateEncodedLocation(responseBodyRuleData.getUserIdLocation());
    if (responseBodyRuleData.hasAuthentication()) {
      authenticationValidator.validate(responseBodyRuleData.getAuthentication());
    }
    // Role not required
  }

  private void validateJwtData(JwtUserAttributionRuleData jwtRuleData) {
    this.validateHeaderLocation(jwtRuleData.getJwtLocation());
    validateNonDefaultPresenceOrThrow(
        jwtRuleData, JwtUserAttributionRuleData.USER_ID_CLAIM_FIELD_NUMBER);
    if (jwtRuleData.hasAuthentication()) {
      authenticationValidator.validate(jwtRuleData.getAuthentication());
    }
    // Role and all encoded locations not required
  }

  private void validateCustomTokenData(final CustomTokenRuleData customTokenData) {
    switch (customTokenData.getLocationCase()) {
      case REQUEST_HEADER_LOCATION:
        validateHeaderLocation(customTokenData.getRequestHeaderLocation());
        break;

      case REQUEST_BODY_LOCATION:
        validateEncodedLocation(customTokenData.getRequestBodyLocation());
        break;

      case LOCATION_NOT_SET:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected token location: " + printMessage(customTokenData))
            .asRuntimeException();
    }

    if (!customTokenData.hasAuthentication()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Specification of authentication is required for custom token")
          .asRuntimeException();
    }

    authenticationValidator.validate(customTokenData.getAuthentication());
  }

  private void validateHeaderLocation(HeaderLocation headerLocation) {
    if (headerLocation.hasParsingTarget()) {
      validateCustomParseTypeLogic(headerLocation.getParsingTarget());
    }
    switch (headerLocation.getLocationCase()) {
      case HEADER_NAME:
        validateNonDefaultPresenceOrThrow(headerLocation, HeaderLocation.HEADER_NAME_FIELD_NUMBER);
        break;
      case COOKIE_NAME:
        validateNonDefaultPresenceOrThrow(headerLocation, HeaderLocation.COOKIE_NAME_FIELD_NUMBER);
        break;
      case LOCATION_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected header location: " + printMessage(headerLocation))
            .asRuntimeException();
    }
  }

  private void validateRuleCondition(RuleCondition ruleCondition) {
    switch (ruleCondition.getConditionCase()) {
      case URL_MATCH_REGEX:
        validateNonDefaultPresenceOrThrow(
            ruleCondition, RuleCondition.URL_MATCH_REGEX_FIELD_NUMBER);
        return;
      case CONDITION_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected rule condition: " + printMessage(ruleCondition))
            .asRuntimeException();
    }
  }

  private void validateEncodedLocation(EncodedLocation encodedLocation) {
    if (encodedLocation.hasParsingTarget()) {
      validateCustomParseTypeLogic(encodedLocation.getParsingTarget());
    }
    switch (encodedLocation.getLocationCase()) {
      case JSON_PATH:
        validateNonDefaultPresenceOrThrow(encodedLocation, EncodedLocation.JSON_PATH_FIELD_NUMBER);
        return;
      case LOCATION_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected encoded location: " + printMessage(encodedLocation))
            .asRuntimeException();
    }
  }

  private void validateValidYaml(String yamlString) {
    try {
      JsonNode node = YAML_OBJECT_MAPPER.readTree(yamlString);
      if (!node.isArray() && !node.isObject()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("YAML of unexpected type: " + node)
            .asRuntimeException();
      }
    } catch (JsonProcessingException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid yaml")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private void validateJsonRuleData(String jsonRuleData) {
    try {
      AttributeRule.Builder attributeRuleBuilder = AttributeRule.newBuilder();
      JSON_FORMAT_PARSER.merge(jsonRuleData, attributeRuleBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid JSON format")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private void validateCustomParseTypeLogic(ParsingTarget parsingTarget) {
    switch (parsingTarget.getTargetCase()) {
      case REGEX_CAPTURE_GROUP:
        validateCaptureGroup(parsingTarget.getRegexCaptureGroup());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected custom parse type logic: " + printMessage(parsingTarget))
            .asRuntimeException();
    }
  }

  private void validateCaptureGroup(String regex) {
    int groupCount = Pattern.compile(regex).groupCount();
    if (groupCount != 1) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Regex should have exactly one capture group but found %d capture groups",
                  groupCount))
          .asRuntimeException();
    }
  }

  private void validateRuleScope(UserAttributionRuleScope scope) {
    if (scope.getScopeCase().equals(UserAttributionRuleScope.ScopeCase.CUSTOM_SCOPE)) {
      UserAttributionRuleScope.CustomScope customScope = scope.getCustomScope();

      if (customScope.equals(UserAttributionRuleScope.CustomScope.getDefaultInstance())) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Custom scope should not be empty")
            .asRuntimeException();
      }

      validateEnvironmentScopes(customScope.getEnvironmentScopesList());
      validateUrlScopes(customScope.getUrlScopesList());
    }
  }

  private void validateEnvironmentScopes(List<UserAttributionRuleScope.EnvironmentScope> scopes) {
    try {
      // validates there are no duplicates present
      Set.of(scopes.toArray());
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
    }
    scopes.forEach(this::validateEnvironmentScope);
  }

  private void validateUrlScopes(List<UserAttributionRuleScope.UrlScope> scopes) {
    try {
      // validates there are no duplicates present
      Set.of(scopes.toArray());
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
    }
    scopes.forEach(this::validateUrlScope);
  }

  private void validateEnvironmentScope(UserAttributionRuleScope.EnvironmentScope scope) {
    if (scope.getEnvironmentName().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Environment should not be empty for environment scope")
          .asRuntimeException();
    }
  }

  private void validateUrlScope(UserAttributionRuleScope.UrlScope scope) {
    if (scope.getUrlMatchRegex().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Url regex should not be empty for url scope")
          .asRuntimeException();
    }

    try {
      Pattern.compile(scope.getUrlMatchRegex());
    } catch (Exception e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Invalid url regex : %s", scope.getUrlMatchRegex()))
          .asRuntimeException();
    }
  }
}
