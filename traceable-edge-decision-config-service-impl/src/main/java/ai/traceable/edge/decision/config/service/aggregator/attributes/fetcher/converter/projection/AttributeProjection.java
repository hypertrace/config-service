package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection;

import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value.ValueProjection;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value.ValueProjectionFactory;
import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import java.util.List;
import java.util.stream.Collectors;

public class AttributeProjection implements Projection {

  private final Attribute attribute;
  private final List<ValueProjection> valueProjections;

  public AttributeProjection(
      ai.traceable.userattribution.config.service.v2.AttributeProjection attributeProjection) {
    this.attribute = attributeProjection.getAttribute();
    this.valueProjections =
        attributeProjection.getValueProjectionsList().stream()
            .map(ValueProjectionFactory::create)
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public String apply(String inputJexlExpression) {
    return valueProjections.stream()
        .reduce(
            generateAttributeExpression(
                inputJexlExpression, attribute), // initial transformation based on attributes
            (currentExpression, valueProjection) ->
                valueProjection.apply(currentExpression), // apply value transformations
            (s1, s2) -> s1);
  }

  private String generateAttributeExpression(String inputJexlExpression, Attribute attribute) {
    switch (attribute.getAttributeCase()) {
      case REQUEST_HEADER:
        return generateKeyMatchExpression(
            String.format("%s.getRequestHeaders()", inputJexlExpression),
            attribute.getRequestHeader());
      case REQUEST_COOKIE:
        return generateKeyMatchExpression(
            String.format("%s.getRequestCookies()", inputJexlExpression),
            attribute.getRequestCookie());
      case REQUEST_QUERY_PARAMETER:
        return generateKeyMatchExpression(
            String.format("%s.getQueryParams()", inputJexlExpression),
            attribute.getRequestQueryParameter());
      case REQUEST_BODY:
        return String.format("%s.getRequestBody()", inputJexlExpression);
      case RESPONSE_HEADER:
      case RESPONSE_COOKIE:
      case RESPONSE_BODY:
      case SPAN_ATTRIBUTE:
      default:
        throw new IllegalArgumentException(
            "Unsupported attribute type: " + attribute.getAttributeCase());
    }
  }

  private String generateKeyMatchExpression(String inputJexlExpression, KeyMatch keyMatch) {
    String key = keyMatch.getMatchKey();
    switch (keyMatch.getOperator()) {
      case KEY_MATCH_OPERATOR_EQUALS:
        return String.format("%s.get('%s')", inputJexlExpression, key);
      case KEY_MATCH_OPERATOR_STARTS_WITH:
        return String.format(
            "map:matchingValue(%s, predicate:startsWith('%s'))", inputJexlExpression, key);
      case KEY_MATCH_OPERATOR_CONTAINS:
        return String.format(
            "map:matchingValue(%s, predicate:contains('%s'))", inputJexlExpression, key);
      case KEY_MATCH_OPERATOR_MATCHES_REGEX:
        return String.format(
            "map:matchingValue(%s, predicate:matchesRegex('%s'))", inputJexlExpression, key);
      default:
        throw new IllegalArgumentException("Unsupported operator: " + keyMatch.getOperator());
    }
  }
}
