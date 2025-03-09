package ai.traceable.jwt.extraction.config.service.converter;

import static ai.traceable.jwt.extraction.config.service.v1.StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import ai.traceable.jwt.extraction.config.service.converter.value.JsonPathProjection;
import ai.traceable.jwt.extraction.config.service.converter.value.RegexCaptureGroupProjection;
import ai.traceable.jwt.extraction.config.service.converter.value.ValueProjection;
import ai.traceable.jwt.extraction.config.service.converter.value.ValueProjectionFactory;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AttributeProjection {
  static final String JEXL_BASE_EXPRESSION = "$s";

  private final JwtExtractionRule jwtExtractionRule;
  private final List<List<ValueProjection>> valueProjectionsList;

  public AttributeProjection(JwtExtractionRule jwtExtractionRule) {
    this.jwtExtractionRule = jwtExtractionRule;
    valueProjectionsList =
        jwtExtractionRule.getInstructionsList().stream()
            .map(ValueProjectionFactory::create)
            .collect(Collectors.toUnmodifiableList());
  }

  public List<SpanAttributeDecoration> getSpanAttributeDecoration() {
    final List<SpanAttributeDecoration> spanAttributeDecorations = new ArrayList<>();
    final AtomicInteger count = new AtomicInteger(0);
    jwtExtractionRule
        .getLocationsList()
        .forEach(
            jwtLocation ->
                valueProjectionsList.forEach(
                    valueProjections -> {
                      count.incrementAndGet();
                      spanAttributeDecorations.add(
                          SpanAttributeDecoration.newBuilder()
                              .setSpanAttributeKey(
                                  DataTransformationConfig.newBuilder()
                                      .setStaticValue(
                                          Value.newBuilder()
                                              .setStringValue(
                                                  "traceableai.jwt."
                                                      + jwtExtractionRule.getId()
                                                      + "."
                                                      + count.get())))
                              .setSpanAttributeValue(
                                  DataTransformationConfig.newBuilder()
                                      .setJexlExpression(
                                          JexlExpressionConfig.newBuilder()
                                              .setJexlExpression(
                                                  this.apply(
                                                      jwtLocation,
                                                      valueProjections,
                                                      JEXL_BASE_EXPRESSION))))
                              .build());
                    }));
    return spanAttributeDecorations;
  }

  public String apply(
      JwtLocation jwtLocation, List<ValueProjection> valueProjections, String inputJexlExpression) {
    return valueProjections.stream()
        .reduce(
            generateAttributeExpression(
                inputJexlExpression, jwtLocation), // initial transformation based on attributes
            (currentExpression, valueProjection) ->
                valueProjection.apply(currentExpression), // apply value transformations
            (s1, s2) -> s1);
  }

  private String generateAttributeExpression(String inputJexlExpression, JwtLocation jwtLocation) {
    String jexlExpression;
    switch (jwtLocation.getLocationCase()) {
      case REQUEST_HEADER:
        jexlExpression =
            generateKeyMatchExpression(
                String.format("%s.getRequestHeaders()", inputJexlExpression),
                jwtLocation.getRequestHeader());
        break;
      case REQUEST_COOKIE:
        jexlExpression =
            generateKeyMatchExpression(
                String.format("%s.getRequestCookies()", inputJexlExpression),
                jwtLocation.getRequestCookie());
        break;
      case QUERY_PARAMETER:
        jexlExpression =
            generateKeyMatchExpression(
                String.format("%s.getQueryParams()", inputJexlExpression),
                jwtLocation.getQueryParameter());
        break;
      case JSON_REQUEST_BODY:
        jexlExpression = String.format("%s.getRequestBody()", inputJexlExpression);
        StringPredicate keyPredicate = jwtLocation.getJsonRequestBody().getKeyPredicate();
        if (keyPredicate.getOperator().equals(RELATIONAL_OPERATOR_EQUALS)) {
          String path = "$." + jwtLocation.getJsonRequestBody().getKeyPredicate().getValue();
          jexlExpression = new JsonPathProjection(path).apply(jexlExpression);
        } else {
          throw new IllegalArgumentException("Unsupported operator: " + keyPredicate.getOperator());
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported attribute type: " + jwtLocation.getLocationCase());
    }
    if (jwtLocation.hasRegexCaptureGroup()) {
      return new RegexCaptureGroupProjection(jwtLocation.getRegexCaptureGroup())
          .apply(jexlExpression);
    }
    return jexlExpression;
  }

  private String generateKeyMatchExpression(String inputJexlExpression, StringPredicate predicate) {
    String key = predicate.getValue();
    switch (predicate.getOperator()) {
      case RELATIONAL_OPERATOR_EQUALS:
        return String.format("%s.get('%s')", inputJexlExpression, key);
      case RELATIONAL_OPERATOR_NOT_EQUALS:
        return String.format(
            "map:matchingValue(%s, predicate:notEquals('%s'))", inputJexlExpression, key);
      case RELATIONAL_OPERATOR_MATCHES_REGEX:
        return String.format(
            "map:matchingValue(%s, predicate:matchesRegex('%s'))", inputJexlExpression, key);
      case RELATIONAL_OPERATOR_NOT_MATCHES_REGEX:
        return String.format(
            "map:matchingValue(%s, predicate:notMatchesRegex('%s'))", inputJexlExpression, key);
      default:
        throw new IllegalArgumentException("Unsupported operator: " + predicate.getOperator());
    }
  }
}
