package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.URL_OR_QUERY_ATTRIBUTE_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryParamLocationTranslator extends AbstractLocationTranslator {
  private static final String QUERY_PARAM_CAPTURE_REGEX = "\\?(.*)$";

  @Override
  public List<LocationTranslationState> translateCase(JwtLocation location)
      throws JwtTranslationException {
    StringPredicate queryParamPredicate = location.getQueryParameter();
    if (queryParamPredicate.getOperator()
        != StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS) {
      throw new JwtTranslationException(
          "Only EQUALS predicate operator supported for query param location");
    }
    return URL_OR_QUERY_ATTRIBUTE_KEYS.stream()
        .map(formatString -> String.format(formatString, queryParamPredicate.getValue()))
        .map(
            qualifiedAttributeKey ->
                new LocationTranslationState(
                    qualifiedAttributeKey,
                    Optional.of(queryParamPredicate.getValue()),
                    getRegexCaptureGroup(location),
                    Optional.of(
                        attributeRule ->
                            extractFromUrl(queryParamPredicate.getValue(), attributeRule))))
        .collect(Collectors.toList());
  }

  private AttributeRule extractFromUrl(String queryParam, AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            AttributeRule.Projector.newBuilder()
                .setRegexCaptureGroupProjector(
                    AttributeRule.Projector.RegexCaptureGroupProjector.newBuilder()
                        .setRegexCaptureGroup(QUERY_PARAM_CAPTURE_REGEX)
                        .setAttributeRule(
                            AttributeRule.newBuilder()
                                .setProjector(
                                    AttributeRule.Projector.newBuilder()
                                        .setUrlEncodedProjector(
                                            AttributeRule.Projector.UrlEncodedProjector.newBuilder()
                                                .setUrlParamRule(
                                                    AttributeRule.Projector.ParsedObjectKeyRule
                                                        .newBuilder()
                                                        .setKey(queryParam)
                                                        .setAttributeRule(childRule))))
                                .build()))
                .build())
        .build();
  }
}
