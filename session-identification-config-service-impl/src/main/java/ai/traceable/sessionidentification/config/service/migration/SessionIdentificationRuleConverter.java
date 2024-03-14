package ai.traceable.sessionidentification.config.service.migration;

import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.LiteralValue;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.MatchOperator;
import ai.traceable.sessionidentification.config.service.v1.ObfuscationStrategy;
import ai.traceable.sessionidentification.config.service.v1.ProjectionRoot;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRuleScope;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRuleStatus;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenValueRule;
import ai.traceable.sessionidentification.config.service.v1.ValueProjection;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionIdentificationRuleConverter {
  private static final String REQUEST_COOKIE_COMPLEX_DATA_KEY = "http.request.header.cookie";
  private static final String RESPONSE_BODY_COMPLEX_DATA_KEY = "http.response.body";
  private static final String EMPTY_STRING = "";
  private static final String ESCAPE_STRING = "\\";

  public Optional<SessionIdentificationRule> convert(RedactionRule redactionRule) {
    if (redactionRule.getComplexData().getKey().equals(RESPONSE_BODY_COMPLEX_DATA_KEY)) {
      return Optional.of(this.buildResponseBodyRule(redactionRule));
    }
    SessionTokenValueRule.Builder tokenValueRule =
        SessionTokenValueRule.newBuilder()
            .setTokenValueProjection(
                ProjectionRoot.newBuilder()
                    .setAttributeProjection(
                        AttributeProjection.newBuilder()
                            .setAttributeKeyMatchCondition(
                                MatchCondition.newBuilder()
                                    .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                    .setMatchValue(
                                        LiteralValue.newBuilder()
                                            .setStringValue(redactionRule.getRegex())))));
    if (redactionRule.getMatchType().equals(MatchType.MATCH_TYPE_HEADER)) {
      return Optional.of(
          buildSessionIdentificationRule(
              redactionRule,
              this.buildRequestHeaderRule(redactionRule, tokenValueRule),
              Optional.empty()));
    }

    if (redactionRule.getComplexData().getKey().equals(REQUEST_COOKIE_COMPLEX_DATA_KEY)) {
      return Optional.of(
          buildSessionIdentificationRule(
              redactionRule,
              this.buildRequestCookieRule(redactionRule, tokenValueRule),
              Optional.empty()));
    }

    log.error("Unrecognized redaction rule {} for converting to Session token rule", redactionRule);
    return Optional.empty();
  }

  private SessionTokenRule buildRequestHeaderRule(
      RedactionRule redactionRule, SessionTokenValueRule.Builder tokenValueRule) {
    SessionTokenRule.Builder tokenRule = SessionTokenRule.newBuilder();

    if (redactionRule.getRedactionStrategy().equals(RedactionStrategy.REDACTION_STRATEGY_HASH)) {
      tokenValueRule.setValueObfuscationStrategy(ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH);
    }
    return tokenRule
        .setRequestSessionTokenDetails(
            RequestSessionTokenDetails.newBuilder()
                .setTokenLocation(
                    RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER))
        .setTokenValueRule(tokenValueRule)
        .build();
  }

  private SessionTokenRule buildRequestCookieRule(
      RedactionRule redactionRule, SessionTokenValueRule.Builder tokenValueRule) {
    SessionTokenRule.Builder tokenRule = SessionTokenRule.newBuilder();
    if (redactionRule.getRedactionStrategy().equals(RedactionStrategy.REDACTION_STRATEGY_HASH)) {
      tokenValueRule.setValueObfuscationStrategy(ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH);
    }
    return tokenRule
        .setRequestSessionTokenDetails(
            RequestSessionTokenDetails.newBuilder()
                .setTokenLocation(
                    RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_COOKIE))
        .setTokenValueRule(tokenValueRule)
        .build();
  }

  private SessionIdentificationRule buildResponseBodyRule(RedactionRule redactionRule) {
    SessionTokenValueRule.Builder tokenValueRule =
        SessionTokenValueRule.newBuilder()
            .setTokenValueProjection(
                ProjectionRoot.newBuilder()
                    .setAttributeProjection(
                        AttributeProjection.newBuilder()
                            .addValueProjectionsInOrder(
                                ValueProjection.newBuilder()
                                    .setJsonPath(
                                        ValueProjection.JsonPathProjection.newBuilder()
                                            .setPath(
                                                // removing escaped characters as old rules have
                                                // only json paths with dot and dollar escaped.
                                                removeEscapeCharacters(
                                                    redactionRule.getRegex()))))));
    if (redactionRule.getRedactionStrategy().equals(RedactionStrategy.REDACTION_STRATEGY_HASH)) {
      tokenValueRule.setValueObfuscationStrategy(ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH);
    }
    Optional<SessionIdentificationRuleScope> scope =
        redactionRule.getConditionsCount() == 1
            ? Optional.of(
                SessionIdentificationRuleScope.newBuilder()
                    .addUrlMatchRegexes(
                        redactionRule.getConditions(0).getAttributeRegexMatch().getRegex())
                    .build())
            : Optional.empty();

    return buildSessionIdentificationRule(
        redactionRule,
        SessionTokenRule.newBuilder()
            .setResponseSessionTokenDetails(
                ResponseSessionTokenDetails.newBuilder()
                    .setTokenLocation(
                        ResponseAttributeKeyLocation.RESPONSE_ATTRIBUTE_KEY_LOCATION_BODY))
            .setTokenValueRule(tokenValueRule)
            .build(),
        scope);
  }

  private SessionIdentificationRule buildSessionIdentificationRule(
      RedactionRule redactionRule,
      SessionTokenRule tokenRule,
      Optional<SessionIdentificationRuleScope> scope) {
    SessionIdentificationRule.Builder builder = SessionIdentificationRule.newBuilder();
    if (!redactionRule.getDescription().isBlank()) {
      builder.setDescription(redactionRule.getDescription());
    }
    scope.ifPresent(builder::setScope);
    return builder
        .setId(redactionRule.getId())
        .setName(redactionRule.getName())
        .setStatus(
            SessionIdentificationRuleStatus.newBuilder()
                .setRuleCreationSource(RuleCreationSource.RULE_CREATION_SOURCE_OLD_API)
                .setDisabled(redactionRule.getDisabled()))
        .addTokenRules(tokenRule)
        .build();
  }

  private String removeEscapeCharacters(String s) {
    return s.replace(ESCAPE_STRING, EMPTY_STRING);
  }
}
