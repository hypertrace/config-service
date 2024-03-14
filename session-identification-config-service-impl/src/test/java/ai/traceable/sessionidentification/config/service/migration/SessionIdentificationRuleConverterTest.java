package ai.traceable.sessionidentification.config.service.migration;

import ai.traceable.sensitivedata.config.service.v1.ComplexData;
import ai.traceable.sensitivedata.config.service.v1.Condition;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SessionIdentificationRuleConverterTest {
  private final SessionIdentificationRuleConverter converter =
      new SessionIdentificationRuleConverter();

  @Test
  void test_header() {
    RedactionRule redactionRule =
        RedactionRule.newBuilder()
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
            .setSessionIdentifier(true)
            .setId("old-rule-id")
            .setName("old rule")
            .setRegex("auth")
            .setMatchType(MatchType.MATCH_TYPE_HEADER)
            .build();

    SessionIdentificationRule expectedRule =
        SessionIdentificationRule.newBuilder()
            .setId("old-rule-id")
            .setName("old rule")
            .setStatus(
                SessionIdentificationRuleStatus.newBuilder()
                    .setRuleCreationSource(RuleCreationSource.RULE_CREATION_SOURCE_OLD_API))
            .addTokenRules(
                SessionTokenRule.newBuilder()
                    .setRequestSessionTokenDetails(
                        RequestSessionTokenDetails.newBuilder()
                            .setTokenLocation(
                                RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER))
                    .setTokenValueRule(
                        SessionTokenValueRule.newBuilder()
                            .setValueObfuscationStrategy(
                                ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH)
                            .setTokenValueProjection(
                                ProjectionRoot.newBuilder()
                                    .setAttributeProjection(
                                        AttributeProjection.newBuilder()
                                            .setAttributeKeyMatchCondition(
                                                MatchCondition.newBuilder()
                                                    .setOperator(
                                                        MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                                    .setMatchValue(
                                                        LiteralValue.newBuilder()
                                                            .setStringValue("auth")))))))
            .build();
    Assertions.assertEquals(expectedRule, converter.convert(redactionRule).get());
  }

  @Test
  void test_cookie() {
    RedactionRule redactionRule =
        RedactionRule.newBuilder()
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
            .setSessionIdentifier(true)
            .setId("old-rule-id")
            .setName("old rule")
            .setRegex("cookie-1")
            .setMatchType(MatchType.MATCH_TYPE_COMPLEX_DATA)
            .setComplexData(ComplexData.newBuilder().setKey("http.request.header.cookie"))
            .build();

    SessionIdentificationRule expectedRule =
        SessionIdentificationRule.newBuilder()
            .setId("old-rule-id")
            .setName("old rule")
            .setStatus(
                SessionIdentificationRuleStatus.newBuilder()
                    .setRuleCreationSource(RuleCreationSource.RULE_CREATION_SOURCE_OLD_API))
            .addTokenRules(
                SessionTokenRule.newBuilder()
                    .setRequestSessionTokenDetails(
                        RequestSessionTokenDetails.newBuilder()
                            .setTokenLocation(
                                RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_COOKIE))
                    .setTokenValueRule(
                        SessionTokenValueRule.newBuilder()
                            .setValueObfuscationStrategy(
                                ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH)
                            .setTokenValueProjection(
                                ProjectionRoot.newBuilder()
                                    .setAttributeProjection(
                                        AttributeProjection.newBuilder()
                                            .setAttributeKeyMatchCondition(
                                                MatchCondition.newBuilder()
                                                    .setOperator(
                                                        MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                                    .setMatchValue(
                                                        LiteralValue.newBuilder()
                                                            .setStringValue("cookie-1")))))))
            .build();
    Assertions.assertEquals(expectedRule, converter.convert(redactionRule).get());
  }

  @Test
  void test_body() {
    RedactionRule redactionRule =
        RedactionRule.newBuilder()
            .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
            .setSessionIdentifier(true)
            .setId("old-rule-id")
            .setName("old rule")
            .setRegex("$.some-json-path")
            .setMatchType(MatchType.MATCH_TYPE_COMPLEX_DATA)
            .addConditions(
                Condition.newBuilder()
                    .setAttributeRegexMatch(
                        Condition.AttributeRegexMatch.newBuilder().setRegex("url-1")))
            .setComplexData(ComplexData.newBuilder().setKey("http.response.body"))
            .build();

    SessionIdentificationRule expectedRule =
        SessionIdentificationRule.newBuilder()
            .setId("old-rule-id")
            .setName("old rule")
            .setStatus(
                SessionIdentificationRuleStatus.newBuilder()
                    .setRuleCreationSource(RuleCreationSource.RULE_CREATION_SOURCE_OLD_API))
            .setScope(SessionIdentificationRuleScope.newBuilder().addUrlMatchRegexes("url-1"))
            .addTokenRules(
                SessionTokenRule.newBuilder()
                    .setResponseSessionTokenDetails(
                        ResponseSessionTokenDetails.newBuilder()
                            .setTokenLocation(
                                ResponseAttributeKeyLocation.RESPONSE_ATTRIBUTE_KEY_LOCATION_BODY))
                    .setTokenValueRule(
                        SessionTokenValueRule.newBuilder()
                            .setValueObfuscationStrategy(
                                ObfuscationStrategy.OBFUSCATION_STRATEGY_HASH)
                            .setTokenValueProjection(
                                ProjectionRoot.newBuilder()
                                    .setAttributeProjection(
                                        AttributeProjection.newBuilder()
                                            .addValueProjectionsInOrder(
                                                ValueProjection.newBuilder()
                                                    .setJsonPath(
                                                        ValueProjection.JsonPathProjection
                                                            .newBuilder()
                                                            .setPath("$.some-json-path")))))))
            .build();
    Assertions.assertEquals(expectedRule, converter.convert(redactionRule).get());
  }
}
