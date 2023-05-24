package ai.traceable.sessionattribution.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.config.utils.RegexValidator;
import ai.traceable.sessionattribution.config.service.v1.AttributeExpiration;
import ai.traceable.sessionattribution.config.service.v1.AttributeProjection;
import ai.traceable.sessionattribution.config.service.v1.LiteralValue;
import ai.traceable.sessionattribution.config.service.v1.MatchCondition;
import ai.traceable.sessionattribution.config.service.v1.MatchOperator;
import ai.traceable.sessionattribution.config.service.v1.Predicate;
import ai.traceable.sessionattribution.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionattribution.config.service.v1.ResponseAccessTokenDetails;
import ai.traceable.sessionattribution.config.service.v1.ResponseRefreshTokenDetails;
import ai.traceable.sessionattribution.config.service.v1.SessionTokenRule;
import ai.traceable.sessionattribution.config.service.v1.SessionTokenValueRule;
import ai.traceable.sessionattribution.config.service.v1.SessionTokenValueTransformation;
import ai.traceable.sessionattribution.config.service.v1.ValueProjection;
import io.grpc.Status;
import java.util.List;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class SessionTokenRuleValidator {
  private static int REGEX_CAPTURE_GROUP_COUNT = 1;

  public void validateTokenRules(List<SessionTokenRule> tokenRules) {
    if (tokenRules.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Session token rule list cannot be empty")
          .asRuntimeException();
    }
    int requestSessionTokenTypeCount = 0;
    for (SessionTokenRule tokenRule : tokenRules) {
      if (tokenRule.hasTokenConditionalPredicate()) {
        validateTokenConditionalPredicate(tokenRule.getTokenConditionalPredicate());
      }
      validateTokenValue(tokenRule.getTokenValueRule());
      switch (tokenRule.getTokenTypeCase()) {
        case REQUEST_SESSION_TOKEN_DETAILS:
          requestSessionTokenTypeCount++;
          validateRequestSessionTokenDetails(tokenRule.getRequestSessionTokenDetails());
          break;
        case RESPONSE_REFRESH_TOKEN_DETAILS:
          validateResponseRefreshTokenDetails(tokenRule.getResponseRefreshTokenDetails());
          break;
        case RESPONSE_ACCESS_TOKEN_DETAILS:
          validateResponseAccessTokenDetails(tokenRule.getResponseAccessTokenDetails());
          break;
        default:
          throw Status.INVALID_ARGUMENT
              .withDescription(String.format("Unexpected token type: %s", printMessage(tokenRule)))
              .asRuntimeException();
      }
    }
    if (requestSessionTokenTypeCount == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "At least one Request Session Token rule must be there in a rule %s", tokenRules))
          .asRuntimeException();
    }
  }

  private void validateTokenConditionalPredicate(Predicate predicate) {
    validateAttributePredicate(predicate.getAttributePredicate());
  }

  private void validateAttributePredicate(Predicate.AttributePredicate predicate) {
    if (predicate.getAttributeKeyLocationCase()
        == Predicate.AttributePredicate.AttributeKeyLocationCase.ATTRIBUTEKEYLOCATION_NOT_SET) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Unexpected AttributeKeyLocation: %s", printMessage(predicate)))
          .asRuntimeException();
    }
    validateAttributeProjection(predicate.getValueProjection());
    validateAttributeMatchCondition(predicate.getValueMatchCondition());
  }

  private void validateTokenValue(SessionTokenValueRule rule) {
    validateAttributeProjection(rule.getTokenValueProjection());
    if (rule.hasValueTransformation()) {
      validateTokenValueTransformation(rule.getValueTransformation());
    }
  }

  private void validateAttributeProjection(AttributeProjection attributeProjection) {
    validateAttributeMatchCondition(attributeProjection.getAttributeKeyMatchCondition());
    validateValueProjections(attributeProjection.getValueProjectionsInOrderList());
  }

  private void validateAttributeMatchCondition(MatchCondition matchCondition) {
    validateNonDefaultPresenceOrThrow(matchCondition, MatchCondition.OPERATOR_FIELD_NUMBER);
    MatchOperator operator = matchCondition.getOperator();
    if (matchCondition.getMatchValue().getValueCase() == LiteralValue.ValueCase.VALUE_NOT_SET
        && operator != MatchOperator.MATCH_OPERATOR_EQUALS
        && operator != MatchOperator.MATCH_OPERATOR_NOT_EQUALS) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Match operator must be equal or not equal for null value: %s",
                  printMessage(matchCondition.getMatchValue())))
          .asRuntimeException();
    }

    if (operator == MatchOperator.MATCH_OPERATOR_MATCHES_REGEX) {
      Status status = RegexValidator.validate(matchCondition.getMatchValue().getStringValue());
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
    }
  }

  private void validateValueProjections(List<ValueProjection> projections) {
    projections.forEach(
        projection -> {
          if (projection.getProjectionCase() == ValueProjection.ProjectionCase.PROJECTION_NOT_SET) {
            throw Status.INVALID_ARGUMENT
                .withDescription(
                    String.format("Unexpected value projection: %s", printMessage(projection)))
                .asRuntimeException();
          }

          if (projection.hasRegexCaptureGroup()) {
            Status status =
                RegexValidator.validateCaptureGroupCount(
                    projection.getRegexCaptureGroup().getRegexCaptureGroup(),
                    REGEX_CAPTURE_GROUP_COUNT);
            if (!status.isOk()) {
              throw status.asRuntimeException();
            }
          }
        });
  }

  private void validateTokenValueTransformation(
      SessionTokenValueTransformation tokenValueTransformation) {
    if (tokenValueTransformation.getTransformationCase()
        == SessionTokenValueTransformation.TransformationCase.TRANSFORMATION_NOT_SET) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Unexpected token value transformation: %s",
                  printMessage(tokenValueTransformation)))
          .asRuntimeException();
    }
  }

  private void validateRequestSessionTokenDetails(RequestSessionTokenDetails tokenDetails) {
    validateNonDefaultPresenceOrThrow(
        tokenDetails, RequestSessionTokenDetails.TOKEN_LOCATION_FIELD_NUMBER);
  }

  private void validateResponseRefreshTokenDetails(ResponseRefreshTokenDetails tokenDetails) {
    validateNonDefaultPresenceOrThrow(
        tokenDetails, ResponseRefreshTokenDetails.TOKEN_LOCATION_FIELD_NUMBER);
    if (tokenDetails.hasAttributeExpiration()) {
      validateAttributeExpiration(tokenDetails.getAttributeExpiration());
    }
  }

  private void validateResponseAccessTokenDetails(ResponseAccessTokenDetails tokenDetails) {
    validateNonDefaultPresenceOrThrow(
        tokenDetails, ResponseAccessTokenDetails.TOKEN_LOCATION_FIELD_NUMBER);
    if (tokenDetails.hasAttributeExpiration()) {
      validateAttributeExpiration(tokenDetails.getAttributeExpiration());
    }
  }

  private void validateAttributeExpiration(AttributeExpiration attributeExpiration) {
    validateNonDefaultPresenceOrThrow(
        attributeExpiration, AttributeExpiration.ATTRIBUTE_KEY_LOCATION_FIELD_NUMBER);
    validateAttributeProjection(attributeExpiration.getAttributeProjection());
    validateNonDefaultPresenceOrThrow(
        attributeExpiration, AttributeExpiration.EXPIRATION_FORMAT_FIELD_NUMBER);
  }
}
