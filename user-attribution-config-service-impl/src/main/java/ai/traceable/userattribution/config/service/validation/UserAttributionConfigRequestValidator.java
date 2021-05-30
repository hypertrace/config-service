package ai.traceable.userattribution.config.service.validation;

import ai.traceable.userattribution.config.service.v1.CreateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.DeleteUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v1.RankUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UpdateUserAttributionRuleRequest;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.CustomUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.EncodedLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.JwtUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RequestHeaderUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.ResponseBodyUserAttributionRuleData;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.RuleCondition;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class UserAttributionConfigRequestValidator {
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

  public void validateOrThrow(
      RequestContext requestContext, GetUserAttributionRulesRequest request) {
    this.validateRequestContext(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, CreateUserAttributionRuleRequest request) {
    this.validateRequestContext(requestContext);
    this.validateNonDefaultPresence(request, CreateUserAttributionRuleRequest.NAME_FIELD_NUMBER);
    this.validateRuleData(request.getData());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateUserAttributionRuleRequest request) {
    this.validateRequestContext(requestContext);
    UserAttributionRule rule = request.getRule();
    this.validateNonDefaultPresence(rule, UserAttributionRule.ID_FIELD_NUMBER);
    this.validateNonDefaultPresence(rule, UserAttributionRule.NAME_FIELD_NUMBER);
    this.validateRuleData(rule.getData());
  }

  public void validateUpdateOrThrow(UserAttributionRule existing, UserAttributionRule updated) {
    if (existing.getRank() != updated.getRank()) {
      throw new IllegalArgumentException(
          "Updated rule must match existing rule rank. Use rank API to adjust ranks");
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteUserAttributionRuleRequest request) {
    this.validateRequestContext(requestContext);
    this.validateNonDefaultPresence(request, DeleteUserAttributionRuleRequest.RULE_ID_FIELD_NUMBER);
  }

  public void validateOrThrow(
      RequestContext requestContext, RankUserAttributionRuleRequest request) {
    this.validateRequestContext(requestContext);
    this.validateNonDefaultPresence(
        request, RankUserAttributionRuleRequest.ID_TO_UPDATE_FIELD_NUMBER);
    if (request.getIdToUpdate().equals(request.getPrecedingRuleId())) {
      throw new IllegalArgumentException(
          "Can't rerank a rule against itself: " + this.printOrToString(request));
    }
  }

  private void validateRequestContext(RequestContext requestContext) {
    if (requestContext.getTenantId().isEmpty()) {
      throw new IllegalArgumentException("Missing expected Tenant ID in request");
    }
  }

  private void validateRuleData(UserAttributionRuleData ruleData) {
    switch (ruleData.getDataCase()) {
      case JWT_DATA:
        this.validateJwtData(ruleData.getJwtData());
        break;
      case BASIC_AUTHENTICATION_DATA:
        // No validation required, no data expected
        break;
      case RESPONSE_BODY_DATA:
        this.validateResponseBodyData(ruleData.getResponseBodyData());
        break;
      case REQUEST_HEADER_DATA:
        this.validateRequestHeaderRuleData(ruleData.getRequestHeaderData());
        break;
      case CUSTOM_DATA:
        this.validateCustomRuleData(ruleData.getCustomData());
        break;
      case DATA_NOT_SET:
      default:
        throw new IllegalArgumentException(
            "Unexpected data case: " + this.printOrToString(ruleData));
    }
  }

  private void validateCustomRuleData(CustomUserAttributionRuleData customRuleData) {
    this.validateNonDefaultPresence(
        customRuleData, CustomUserAttributionRuleData.YAML_FIELD_NUMBER);
  }

  private void validateRequestHeaderRuleData(
      RequestHeaderUserAttributionRuleData requestHeaderRuleData) {
    this.validateHeaderLocation(requestHeaderRuleData.getUserIdLocation());
    // Role not required
  }

  private void validateResponseBodyData(ResponseBodyUserAttributionRuleData responseBodyRuleData) {
    this.validateRuleCondition(responseBodyRuleData.getCondition());
    this.validateEncodedLocation(responseBodyRuleData.getUserIdLocation());
    // Role not required
  }

  private void validateJwtData(JwtUserAttributionRuleData jwtRuleData) {
    this.validateHeaderLocation(jwtRuleData.getJwtLocation());
    this.validateNonDefaultPresence(
        jwtRuleData, JwtUserAttributionRuleData.USER_ID_CLAIM_FIELD_NUMBER);
    // Role and all encoded locations not required
  }

  private void validateHeaderLocation(HeaderLocation headerLocation) {
    switch (headerLocation.getLocationCase()) {
      case HEADER_NAME:
        this.validateNonDefaultPresence(headerLocation, HeaderLocation.HEADER_NAME_FIELD_NUMBER);
        break;
      case COOKIE_NAME:
        this.validateNonDefaultPresence(headerLocation, HeaderLocation.COOKIE_NAME_FIELD_NUMBER);
        break;
      case LOCATION_NOT_SET:
      default:
        throw new IllegalArgumentException(
            "Unexpected header location: " + this.printOrToString(headerLocation));
    }
  }

  private void validateRuleCondition(RuleCondition ruleCondition) {
    switch (ruleCondition.getConditionCase()) {
      case URL_MATCH_REGEX:
        this.validateNonDefaultPresence(ruleCondition, RuleCondition.URL_MATCH_REGEX_FIELD_NUMBER);
        return;
      case CONDITION_NOT_SET:
      default:
        throw new IllegalArgumentException(
            "Unexpected rule condition: " + this.printOrToString(ruleCondition));
    }
  }

  private void validateEncodedLocation(EncodedLocation encodedLocation) {
    switch (encodedLocation.getLocationCase()) {
      case JSON_PATH:
        this.validateNonDefaultPresence(encodedLocation, EncodedLocation.JSON_PATH_FIELD_NUMBER);
        return;
      case LOCATION_NOT_SET:
      default:
        throw new IllegalArgumentException(
            "Unexpected encoded location: " + this.printOrToString(encodedLocation));
    }
  }

  private <T extends Message> void validateNonDefaultPresence(T source, int fieldNumber) {
    FieldDescriptor descriptor = source.getDescriptorForType().findFieldByNumber(fieldNumber);
    if (!source.hasField(descriptor)
        || source.getField(descriptor).equals(descriptor.getDefaultValue())) {
      throw new IllegalArgumentException(
          String.format(
              "Expected field value %s but not present:\n %s",
              descriptor.getFullName(), this.printOrToString(source)));
    }
  }

  private String printOrToString(Message message) {
    try {
      return JSON_PRINTER.print(message);
    } catch (Exception exception) {
      return message.toString();
    }
  }
}
