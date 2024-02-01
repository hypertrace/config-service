package ai.traceable.sessionidentification.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.config.utils.JsonPathValidator;
import ai.traceable.config.utils.RegexValidator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.CustomProjection;
import ai.traceable.sessionidentification.config.service.v1.LiteralValue;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.MatchOperator;
import ai.traceable.sessionidentification.config.service.v1.Predicate;
import ai.traceable.sessionidentification.config.service.v1.ProjectionRoot;
import ai.traceable.sessionidentification.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeExpiration;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenValueRule;
import ai.traceable.sessionidentification.config.service.v1.ValueProjection;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import java.util.List;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class SessionTokenRuleValidator {
  private static final int REGEX_CAPTURE_GROUP_COUNT = 1;

  public void validateTokenRules(List<SessionTokenRule> tokenRules) {
    if (tokenRules.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Session token rule list cannot be empty")
          .asRuntimeException();
    }
    for (SessionTokenRule tokenRule : tokenRules) {
      if (tokenRule.hasTokenConditionalPredicate()) {
        validateTokenConditionalPredicate(tokenRule.getTokenConditionalPredicate());
      }
      validateTokenValue(tokenRule.getTokenValueRule());
      switch (tokenRule.getTokenTypeCase()) {
        case REQUEST_SESSION_TOKEN_DETAILS:
          validateRequestSessionTokenDetails(tokenRule.getRequestSessionTokenDetails());
          break;
        case RESPONSE_SESSION_TOKEN_DETAILS:
          validateResponseSessionTokenDetails(tokenRule.getResponseSessionTokenDetails());
          break;
      }
    }
  }

  private void validateTokenConditionalPredicate(Predicate predicate) {
    if (predicate.hasAttributePredicate()) {
      validateAttributePredicate(predicate.getAttributePredicate());
    } else if (predicate.hasCustomPredicate()) {
      validateCustomProjection(predicate.getCustomPredicate());
    }
  }

  private void validateAttributePredicate(Predicate.AttributePredicate predicate) {
    if (predicate.getAttributeKeyLocationCase()
        == Predicate.AttributePredicate.AttributeKeyLocationCase.ATTRIBUTEKEYLOCATION_NOT_SET) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Unexpected AttributeKeyLocation: %s", printMessage(predicate)))
          .asRuntimeException();
    }
    validateAttributeMatchCondition(predicate.getValueMatchCondition());
  }

  private void validateTokenValue(SessionTokenValueRule rule) {
    validateProjectionRoot(rule.getTokenValueProjection());
  }

  private void validateProjectionRoot(ProjectionRoot projectionRoot) {
    switch (projectionRoot.getProjectionCase()) {
      case ATTRIBUTE_PROJECTION:
        validateAttributeProjection(projectionRoot.getAttributeProjection());
        break;
      case CUSTOM_PROJECTION:
        validateCustomProjection(projectionRoot.getCustomProjection());
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Projection root can not be empty: %s", printMessage(projectionRoot)))
            .asRuntimeException();
    }
  }

  private void validateCustomProjection(CustomProjection customProjection) {

    AttributeRule.Projector.Builder builder = AttributeRule.Projector.newBuilder();
    try {
      JsonFormat.parser().merge(customProjection.getCustomProjection(), builder);
    } catch (InvalidProtocolBufferException e) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT.withDescription(
                  String.format(
                      "Invalid custom projection: %s", customProjection.getCustomProjection())))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  private void validateAttributeProjection(AttributeProjection attributeProjection) {
    if (attributeProjection.hasAttributeKeyMatchCondition()) {
      validateAttributeMatchCondition(attributeProjection.getAttributeKeyMatchCondition());
    }
    validateValueProjections(attributeProjection.getValueProjectionsInOrderList());
  }

  private void validateAttributeMatchCondition(MatchCondition matchCondition) {
    validateNonDefaultPresenceOrThrow(matchCondition, MatchCondition.OPERATOR_FIELD_NUMBER);
    MatchOperator operator = matchCondition.getOperator();
    if (matchCondition.getMatchValue().getValueCase() == LiteralValue.ValueCase.VALUE_NOT_SET
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
          switch (projection.getProjectionCase()) {
            case PROJECTION_NOT_SET:
              throw Status.INVALID_ARGUMENT
                  .withDescription(
                      String.format("Unexpected value projection: %s", printMessage(projection)))
                  .asRuntimeException();
            case REGEX_CAPTURE_GROUP:
              Status status =
                  RegexValidator.validateCaptureGroupCount(
                      projection.getRegexCaptureGroup().getRegexCaptureGroup(),
                      REGEX_CAPTURE_GROUP_COUNT);
              if (!status.isOk()) {
                throw status.asRuntimeException();
              }
              break;
            case JSON_PATH:
              validateNonDefaultPresenceOrThrow(
                  projection.getJsonPath(), ValueProjection.JsonPathProjection.PATH_FIELD_NUMBER);
              status = JsonPathValidator.validate(projection.getJsonPath().getPath());
              if (!status.isOk()) {
                throw status.asRuntimeException();
              }
          }
        });
  }

  private void validateRequestSessionTokenDetails(RequestSessionTokenDetails tokenDetails) {
    validateNonDefaultPresenceOrThrow(
        tokenDetails, RequestSessionTokenDetails.TOKEN_LOCATION_FIELD_NUMBER);
  }

  private void validateResponseSessionTokenDetails(ResponseSessionTokenDetails tokenDetails) {
    validateNonDefaultPresenceOrThrow(
        tokenDetails, ResponseSessionTokenDetails.TOKEN_LOCATION_FIELD_NUMBER);
    if (tokenDetails.hasResponseAttributeExpiration()) {
      validateAttributeExpiration(tokenDetails.getResponseAttributeExpiration());
    }
  }

  private void validateAttributeExpiration(ResponseAttributeExpiration attributeExpiration) {
    validateNonDefaultPresenceOrThrow(
        attributeExpiration, ResponseAttributeExpiration.ATTRIBUTE_KEY_LOCATION_FIELD_NUMBER);
    validateProjectionRoot(attributeExpiration.getProjectionRoot());
    validateNonDefaultPresenceOrThrow(
        attributeExpiration, ResponseAttributeExpiration.EXPIRATION_FORMAT_FIELD_NUMBER);
  }
}
