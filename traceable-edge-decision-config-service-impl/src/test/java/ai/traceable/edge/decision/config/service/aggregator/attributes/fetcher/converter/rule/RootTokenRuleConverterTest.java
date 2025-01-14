package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.KeyMatchOperator;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import ai.traceable.userattribution.config.service.v2.ValueProjection.RegexCaptureGroupProjection;
import org.junit.jupiter.api.Test;

class RootTokenRuleConverterTest {

  @Test
  void testConvert_withDefaultInstance() {
    UserAttributionRootTokenRule defaultRule = UserAttributionRootTokenRule.getDefaultInstance();
    RootTokenRuleConverter converter = new RootTokenRuleConverter(defaultRule);
    String inputJexl = "inputJexlExpression";

    String result = converter.convert(inputJexl);
    assertEquals(inputJexl, result);
  }

  @Test
  void testConvert_withNonDefaultInstance() {
    ai.traceable.userattribution.config.service.v2.AttributeProjection attributeProjection =
        ai.traceable.userattribution.config.service.v2.AttributeProjection.newBuilder()
            .setAttribute(
                Attribute.newBuilder()
                    .setRequestHeader(
                        KeyMatch.newBuilder()
                            .setOperator(KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS)
                            .setMatchKey("authorization")))
            .addValueProjections(
                ValueProjection.newBuilder()
                    .setRegexCaptureGroup(RegexCaptureGroupProjection.newBuilder().setRegex("a")))
            .build();

    UserAttributionRootTokenRule nonDefaultRule =
        UserAttributionRootTokenRule.newBuilder()
            .setAttributeProjection(attributeProjection)
            .build();

    RootTokenRuleConverter converter = new RootTokenRuleConverter(nonDefaultRule);
    String inputJexl = "$s";
    String result = converter.convert(inputJexl);
    assertEquals("$s.getRequestHeaders().get('authorization').replaceAll(\"a\", \"$1\")", result);
  }
}
