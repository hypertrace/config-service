package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_BODY_KEYS;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_COOKIE_HEADER_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.RESPONSE_BODY_KEYS;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.RESPONSE_COOKIE_HEADER_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.URL_OR_QUERY_ATTRIBUTE_KEYS;
import static ai.traceable.userattribution.config.service.v2.KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ParsedObjectKeyRule;
import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.KeyMatchOperator;
import ai.traceable.userattribution.config.service.v2.Predicate.AttributePredicate;
import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
class AttributeTranslator {
  private static final String QUERY_PARAM_CAPTURE_REGEX = "\\?(.*)$";

  private final AttributeRuleBuilder attributeRuleBuilder;
  private final AttributeKeysExtractor attributeKeysExtractor;
  private final MatchConditionTranslator matchConditionTranslator;
  private final ValueProjectionsTranslator valueProjectionsTranslator;

  List<Projector> translate(AttributePredicate attributePredicate) {
    return translateAttribute(
            attributePredicate.getAttributeProjection().getAttribute(),
            valueProjectionsTranslator.translateValueProjections(
                attributePredicate.getAttributeProjection().getValueProjectionsList(),
                addMatchConditionForPredicate(attributePredicate).build()))
        .map(AttributeRule::getProjector)
        .collect(Collectors.toUnmodifiableList());
  }

  Stream<AttributeRule> translateAttribute(Attribute attribute, AttributeRule childAttributeRule) {
    switch (attribute.getAttributeCase()) {
      case REQUEST_HEADER:
        return translateAttributeKeys(
            attributeKeysExtractor.getRequestHeaderAttributeKeys(attribute.getRequestHeader()),
            attribute.getRequestHeader().getOperator(),
            childAttributeRule);
      case REQUEST_COOKIE:
        return translateCookieAttributeKey(
            REQUEST_COOKIE_HEADER_KEY, attribute.getRequestCookie(), childAttributeRule);
      case REQUEST_QUERY_PARAMETER:
        return translateQueryAttributeKeys(
            URL_OR_QUERY_ATTRIBUTE_KEYS, attribute.getRequestQueryParameter(), childAttributeRule);
      case REQUEST_BODY:
        return translateAttributeKeys(REQUEST_BODY_KEYS, childAttributeRule);
      case RESPONSE_HEADER:
        return translateAttributeKeys(
            attributeKeysExtractor.getResponseHeaderAttributeKeys(attribute.getResponseHeader()),
            attribute.getResponseHeader().getOperator(),
            childAttributeRule);
      case RESPONSE_COOKIE:
        return translateCookieAttributeKey(
            RESPONSE_COOKIE_HEADER_KEY, attribute.getResponseCookie(), childAttributeRule);
      case RESPONSE_BODY:
        return translateAttributeKeys(RESPONSE_BODY_KEYS, childAttributeRule);
      case SPAN_ATTRIBUTE:
        return translateAttributeKeys(
            List.of(attribute.getSpanAttribute().getMatchKey()),
            attribute.getSpanAttribute().getOperator(),
            childAttributeRule);
      default:
        throw Status.INTERNAL
            .withDescription(
                String.format("Unknown attribute case: %s", attribute.getAttributeCase()))
            .asRuntimeException();
    }
  }

  private Stream<AttributeRule> translateAttributeKeys(
      List<String> attributeKeys, AttributeRule childAttributeRule) {
    return attributeKeys.stream()
        .map(
            attributeKey ->
                attributeRuleBuilder.buildRuleForAttribute(attributeKey, childAttributeRule));
  }

  private Stream<AttributeRule> translateAttributeKeys(
      List<String> attributeKeys,
      KeyMatchOperator keyMatchOperator,
      AttributeRule childAttributeRule) {
    if (keyMatchOperator.equals(KEY_MATCH_OPERATOR_EQUALS)) {
      return attributeKeys.stream()
          .map(
              attributeKey ->
                  attributeRuleBuilder.buildRuleForAttribute(attributeKey, childAttributeRule));
    } else {
      return attributeKeys.stream()
          .map(
              attributeKeyRegex ->
                  Predicate.StringPredicate.newBuilder()
                      .setOperator(Predicate.ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX)
                      .setValue(attributeKeyRegex)
                      .build())
          .map(
              predicate ->
                  attributeRuleBuilder.buildRuleForAttribute(predicate, childAttributeRule));
    }
  }

  private Stream<AttributeRule> translateCookieAttributeKey(
      String attributeKey, KeyMatch keyMatch, AttributeRule childAttributeRule) {
    return Stream.of(
        attributeRuleBuilder.buildRuleForAttribute(
            attributeKey,
            keyMatch.getOperator().equals(KEY_MATCH_OPERATOR_EQUALS)
                ? attributeRuleBuilder.buildRuleForCookie(
                    keyMatch.getMatchKey(), childAttributeRule)
                : attributeRuleBuilder.buildRuleForCookie(
                    matchConditionTranslator.buildStringPredicate(
                        keyMatch.getOperator(), keyMatch.getMatchKey()),
                    childAttributeRule)));
  }

  private Stream<AttributeRule> translateQueryAttributeKeys(
      List<String> attributeKeys, KeyMatch keyMatch, AttributeRule childAttributeRule) {
    return attributeKeys.stream()
        .map(
            attributeKey ->
                AttributeRule.newBuilder()
                    .setProjector(
                        AttributeRule.Projector.newBuilder()
                            .setAttributeProjector(
                                AttributeRule.Projector.AttributeProjector.newBuilder()
                                    .setAttributeKey(attributeKey)
                                    .setAttributeRule(
                                        AttributeRule.newBuilder()
                                            .setProjector(
                                                AttributeRule.Projector.newBuilder()
                                                    .setRegexCaptureGroupProjector(
                                                        AttributeRule.Projector
                                                            .RegexCaptureGroupProjector.newBuilder()
                                                            .setRegexCaptureGroup(
                                                                QUERY_PARAM_CAPTURE_REGEX)
                                                            .setAttributeRule(
                                                                AttributeRule.newBuilder()
                                                                    .setProjector(
                                                                        AttributeRule.Projector
                                                                            .newBuilder()
                                                                            .setUrlEncodedProjector(
                                                                                AttributeRule
                                                                                    .Projector
                                                                                    .UrlEncodedProjector
                                                                                    .newBuilder()
                                                                                    .setUrlParamRule(
                                                                                        buildParsedObjectKeyRule(
                                                                                            keyMatch,
                                                                                            childAttributeRule))))))))))
                    .build());
  }

  private ParsedObjectKeyRule.Builder buildParsedObjectKeyRule(
      KeyMatch keyMatch, AttributeRule childAttributeRule) {
    if (keyMatch.getOperator().equals(KEY_MATCH_OPERATOR_EQUALS)) {
      return ParsedObjectKeyRule.newBuilder()
          .setKey(keyMatch.getMatchKey())
          .setAttributeRule(childAttributeRule);
    } else {
      return ParsedObjectKeyRule.newBuilder()
          .setKeyPredicate(matchConditionTranslator.translate(keyMatch))
          .setAttributeRule(childAttributeRule);
    }
  }

  private AttributeRule.Builder addMatchConditionForPredicate(
      AttributePredicate attributePredicate) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setConditionalProjector(
                    ConditionalProjector.newBuilder()
                        .setPredicate(
                            Predicate.newBuilder()
                                .setCurrentValuePredicate(
                                    matchConditionTranslator.translate(
                                        attributePredicate.getAttributeValueMatchCondition())))));
  }
}
