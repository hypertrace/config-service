package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.userattribution.config.service.v2.*;
import ai.traceable.userattribution.config.service.v2.ValueProjection.RegexCaptureGroupProjection;
import org.junit.jupiter.api.Test;

class TokenRuleConverterTest {

  @Test
  void testConvert_withDefaultInstance() {
    UserAttributionTokenRule defaultRule = UserAttributionTokenRule.getDefaultInstance();
    UserAttributionRootTokenRule rootRule = UserAttributionRootTokenRule.getDefaultInstance();

    TokenRuleConverter converter = new TokenRuleConverter(defaultRule, rootRule);
    String inputJexl = "inputJexlExpression";

    String result = converter.convert(inputJexl);
    assertEquals(inputJexl, result);
  }

  @Test
  void testConvert_withAttributeProjection() {
    AttributeProjection attributeProjection =
        AttributeProjection.newBuilder()
            .setAttribute(
                Attribute.newBuilder()
                    .setRequestHeader(
                        KeyMatch.newBuilder()
                            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS)
                            .setMatchKey("authorization")))
            .build();

    UserAttributionTokenRule tokenRule =
        UserAttributionTokenRule.newBuilder().setAttributeProjection(attributeProjection).build();
    UserAttributionRootTokenRule rootRule = UserAttributionRootTokenRule.getDefaultInstance();

    TokenRuleConverter converter = new TokenRuleConverter(tokenRule, rootRule);
    String inputJexl = "$s";
    String result = converter.convert(inputJexl);

    assertEquals("$s.getRequestHeaders().get('authorization')", result);
  }

  @Test
  void testConvert_withLiteralValueProjection() {
    LiteralValueProjection literalValueProjection =
        LiteralValueProjection.newBuilder()
            .setLiteralValue(LiteralValue.newBuilder().setStringValue("static_value"))
            .build();

    UserAttributionTokenRule tokenRule =
        UserAttributionTokenRule.newBuilder()
            .setLiteralValueProjection(literalValueProjection)
            .build();
    UserAttributionRootTokenRule rootRule = UserAttributionRootTokenRule.getDefaultInstance();

    TokenRuleConverter converter = new TokenRuleConverter(tokenRule, rootRule);
    String inputJexl = "$s";
    String result = converter.convert(inputJexl);

    assertEquals("static_value", result);
  }

  @Test
  void testConvert_withRootRelativeProjection() {
    RootRelativeProjection rootRelativeProjection =
        RootRelativeProjection.newBuilder()
            .addValueProjections(
                ValueProjection.newBuilder()
                    .setRegexCaptureGroup(RegexCaptureGroupProjection.newBuilder().setRegex("a")))
            .build();

    UserAttributionTokenRule tokenRule =
        UserAttributionTokenRule.newBuilder()
            .setRootRelativeProjection(rootRelativeProjection)
            .build();

    UserAttributionRootTokenRule rootRule =
        UserAttributionRootTokenRule.newBuilder()
            .setAttributeProjection(
                AttributeProjection.newBuilder()
                    .setAttribute(
                        Attribute.newBuilder()
                            .setRequestHeader(
                                KeyMatch.newBuilder()
                                    .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS)
                                    .setMatchKey("auth"))))
            .build();

    TokenRuleConverter converter = new TokenRuleConverter(tokenRule, rootRule);
    String inputJexl = "$s";
    String result = converter.convert(inputJexl);

    assertEquals("$s.getRequestHeaders().get('auth').replaceAll(\"a\", \"$1\")", result);
  }
}
