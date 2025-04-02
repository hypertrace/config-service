package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ValueProjectionTest {
  @Test
  void regexProjectorTest() {
    String input = "$s.getHeaders().get('authorization')";
    ValueProjection projector = new RegexCaptureGroupProjection("(?i)^Basic:? (.*)");
    String jexlExpression = input + ".replaceAll(\"(?i)^Basic:? (.*)\", \"$1\")";
    assertEquals(jexlExpression, projector.apply(input));
  }

  @Test
  void base64ProjectorTest() {
    String input = "$s.getHeaders().get('authorization')";
    ValueProjection projector = new Base64Projection();
    String jexlExpression = "traceableTransformUtils:base64Decode(" + input + ")";
    assertEquals(jexlExpression, projector.apply(input));
  }

  @Test
  void jwtProjectorTest() {
    String input = "$s.getHeaders().get('authorization')";
    ValueProjection projector = new JwtClaimProjection("sub");
    String jexlExpression =
        "traceableTransformUtils:extractJWTPath(" + input + ", \"sub\", \"PAYLOAD\")";
    assertEquals(jexlExpression, projector.apply(input));
  }

  @Test
  void jsonPathProjectorTest() {
    String input = "$s.getHeaders().get('authorization')";
    ValueProjection projector = new JsonPathProjection("apple.mango");
    String jexlExpression =
        "traceableTransformUtils:extractJsonPathValue(" + input + ", \"apple.mango\")";
    assertEquals(jexlExpression, projector.apply(input));
  }

  @Test
  void urlDecodeProjectorTest() {
    String input = "$s.getHeaders().get('authorization')";
    ValueProjection projector = new UrlDecodeStringProjection(true);
    String jexlExpression = "traceableTransformUtils:urlDecode(" + input + ", true)";
    assertEquals(jexlExpression, projector.apply(input));

    projector = new UrlDecodeStringProjection(false);
    jexlExpression = "traceableTransformUtils:urlDecode(" + input + ", false)";
    assertEquals(jexlExpression, projector.apply(input));
  }
}
