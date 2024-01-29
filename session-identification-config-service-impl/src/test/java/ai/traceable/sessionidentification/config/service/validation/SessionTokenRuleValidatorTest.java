package ai.traceable.sessionidentification.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.CustomProjection;
import ai.traceable.sessionidentification.config.service.v1.LiteralValue;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.MatchOperator;
import ai.traceable.sessionidentification.config.service.v1.Predicate;
import ai.traceable.sessionidentification.config.service.v1.ProjectionRoot;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeExpiration;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenValueRule;
import ai.traceable.sessionidentification.config.service.v1.ValueProjection;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionTokenRuleValidatorTest {
  private static final SessionTokenValueRule TOKEN_VALUE_RULE =
      SessionTokenValueRule.newBuilder()
          .setTokenValueProjection(
              ProjectionRoot.newBuilder()
                  .setAttributeProjection(
                      AttributeProjection.newBuilder()
                          .setAttributeKeyMatchCondition(
                              MatchCondition.newBuilder()
                                  .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                  .setMatchValue(
                                      LiteralValue.newBuilder().setStringValue("auth")))))
          .build();
  @InjectMocks SessionTokenRuleValidator tokenRuleValidator;

  @Test
  void validateProjectionRoot() {
    assertInvalidArgStatusContaining(
        "Projection root can not be empty",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(SessionTokenValueRule.getDefaultInstance())
                        .setRequestSessionTokenDetails(
                            RequestSessionTokenDetails.newBuilder()
                                .setTokenLocation(
                                    RequestAttributeKeyLocation
                                        .REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER))
                        .build())));
  }

  @Test
  void validateCustomProjection() {
    assertInvalidArgStatusContaining(
        "Invalid custom projection json",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(
                            SessionTokenValueRule.newBuilder()
                                .setTokenValueProjection(
                                    ProjectionRoot.newBuilder()
                                        .setCustomProjection(
                                            CustomProjection.newBuilder().setCustomProjection(""))))
                        .setRequestSessionTokenDetails(
                            RequestSessionTokenDetails.newBuilder()
                                .setTokenLocation(
                                    RequestAttributeKeyLocation
                                        .REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER))
                        .build())));
  }

  @Test
  void validateValueProjection() {
    assertInvalidArgStatusContaining(
        "Unexpected value projection",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(
                            TOKEN_VALUE_RULE.toBuilder()
                                .setTokenValueProjection(
                                    ProjectionRoot.newBuilder()
                                        .setAttributeProjection(
                                            AttributeProjection.newBuilder()
                                                .addValueProjectionsInOrder(
                                                    ValueProjection.getDefaultInstance())
                                                .setAttributeKeyMatchCondition(
                                                    MatchCondition.newBuilder()
                                                        .setOperator(
                                                            MatchOperator.MATCH_OPERATOR_EQUALS)
                                                        .setMatchValue(
                                                            LiteralValue.newBuilder()
                                                                .setStringValue("auth"))))))
                        .setRequestSessionTokenDetails(
                            RequestSessionTokenDetails.newBuilder()
                                .setTokenLocation(
                                    RequestAttributeKeyLocation
                                        .REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER))
                        .build())));

    assertInvalidArgStatusContaining(
        "Invalid Regex pattern:",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(
                            TOKEN_VALUE_RULE.toBuilder()
                                .setTokenValueProjection(
                                    ProjectionRoot.newBuilder()
                                        .setAttributeProjection(
                                            AttributeProjection.newBuilder()
                                                .addValueProjectionsInOrder(
                                                    ValueProjection.newBuilder()
                                                        .setRegexCaptureGroup(
                                                            ValueProjection
                                                                .RegexCaptureGroupProjection
                                                                .newBuilder()
                                                                .setRegexCaptureGroup("+")))
                                                .setAttributeKeyMatchCondition(
                                                    MatchCondition.newBuilder()
                                                        .setOperator(
                                                            MatchOperator.MATCH_OPERATOR_EQUALS)
                                                        .setMatchValue(
                                                            LiteralValue.newBuilder()
                                                                .setStringValue("auth"))))))
                        .setRequestSessionTokenDetails(
                            RequestSessionTokenDetails.newBuilder()
                                .setTokenLocation(
                                    RequestAttributeKeyLocation
                                        .REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER))
                        .build())));

    assertInvalidArgStatusContaining(
        "Regex group count should be: 1",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(
                            TOKEN_VALUE_RULE.toBuilder()
                                .setTokenValueProjection(
                                    ProjectionRoot.newBuilder()
                                        .setAttributeProjection(
                                            AttributeProjection.newBuilder()
                                                .addValueProjectionsInOrder(
                                                    ValueProjection.newBuilder()
                                                        .setRegexCaptureGroup(
                                                            ValueProjection
                                                                .RegexCaptureGroupProjection
                                                                .newBuilder()
                                                                .setRegexCaptureGroup(
                                                                    "((A)(B(C)))")))
                                                .setAttributeKeyMatchCondition(
                                                    MatchCondition.newBuilder()
                                                        .setOperator(
                                                            MatchOperator.MATCH_OPERATOR_EQUALS)
                                                        .setMatchValue(
                                                            LiteralValue.newBuilder()
                                                                .setStringValue("auth"))))))
                        .setRequestSessionTokenDetails(
                            RequestSessionTokenDetails.newBuilder()
                                .setTokenLocation(
                                    RequestAttributeKeyLocation
                                        .REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER))
                        .build())));

    assertInvalidArgStatusContaining(
        "Invalid json path:",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(
                            TOKEN_VALUE_RULE.toBuilder()
                                .setTokenValueProjection(
                                    ProjectionRoot.newBuilder()
                                        .setAttributeProjection(
                                            AttributeProjection.newBuilder()
                                                .addValueProjectionsInOrder(
                                                    ValueProjection.newBuilder()
                                                        .setJsonPath(
                                                            ValueProjection.JsonPathProjection
                                                                .newBuilder()
                                                                .setPath("$jsonpath")))
                                                .setAttributeKeyMatchCondition(
                                                    MatchCondition.newBuilder()
                                                        .setOperator(
                                                            MatchOperator.MATCH_OPERATOR_EQUALS)
                                                        .setMatchValue(
                                                            LiteralValue.newBuilder()
                                                                .setStringValue("auth"))))))
                        .setRequestSessionTokenDetails(
                            RequestSessionTokenDetails.newBuilder()
                                .setTokenLocation(
                                    RequestAttributeKeyLocation
                                        .REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER))
                        .build())));
  }

  @Test
  void validateAttributePredicate() {
    assertInvalidArgStatusContaining(
        "Unexpected AttributeKeyLocation",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(TOKEN_VALUE_RULE)
                        .setTokenConditionalPredicate(
                            Predicate.newBuilder()
                                .setAttributePredicate(
                                    Predicate.AttributePredicate.getDefaultInstance())
                                .build())
                        .setRequestSessionTokenDetails(
                            RequestSessionTokenDetails.newBuilder()
                                .setTokenLocation(
                                    RequestAttributeKeyLocation
                                        .REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER))
                        .build())));
  }

  @Test
  void validateRequestSessionTokenDetails() {
    assertInvalidArgStatusContaining(
        "RequestSessionTokenDetails.token_location but not present",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(TOKEN_VALUE_RULE)
                        .setRequestSessionTokenDetails(RequestSessionTokenDetails.newBuilder())
                        .build())));
  }

  @Test
  void validateResponseAccessTokenDetails() {
    assertInvalidArgStatusContaining(
        "ResponseSessionTokenDetails.token_location but not present",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(TOKEN_VALUE_RULE)
                        .setResponseSessionTokenDetails(ResponseSessionTokenDetails.newBuilder())
                        .build())));
  }

  @Test
  void validateAttributeExpiration() {
    assertInvalidArgStatusContaining(
        "AttributeExpiration.expiration_format but not present",
        () ->
            tokenRuleValidator.validateTokenRules(
                List.of(
                    SessionTokenRule.newBuilder()
                        .setTokenValueRule(TOKEN_VALUE_RULE)
                        .setResponseSessionTokenDetails(
                            ResponseSessionTokenDetails.newBuilder()
                                .setResponseAttributeExpiration(
                                    ResponseAttributeExpiration.newBuilder()
                                        .setProjectionRoot(
                                            ProjectionRoot.newBuilder()
                                                .setAttributeProjection(
                                                    AttributeProjection.newBuilder()
                                                        .setAttributeKeyMatchCondition(
                                                            MatchCondition.newBuilder()
                                                                .setOperator(
                                                                    MatchOperator
                                                                        .MATCH_OPERATOR_EQUALS)
                                                                .setMatchValue(
                                                                    LiteralValue.newBuilder()
                                                                        .setStringValue("auth")))))
                                        .setAttributeKeyLocation(
                                            ResponseAttributeKeyLocation
                                                .RESPONSE_ATTRIBUTE_KEY_LOCATION_HEADER)
                                        .build())
                                .setTokenLocation(
                                    ResponseAttributeKeyLocation
                                        .RESPONSE_ATTRIBUTE_KEY_LOCATION_HEADER))
                        .build())));
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
