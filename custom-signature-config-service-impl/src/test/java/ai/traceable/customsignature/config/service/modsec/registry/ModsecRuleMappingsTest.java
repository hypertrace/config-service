package ai.traceable.customsignature.config.service.modsec.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import org.junit.jupiter.api.Test;

public class ModsecRuleMappingsTest {

  private final ModsecRuleMappings modsecRuleMappings = new ModsecRuleMappings();

  @Test
  public void test_getVariablePlusOperatorString_matchExpression() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariablePlusOperatorString(
                MatchCategory.MATCH_CATEGORY_REQUEST,
                MatchKey.MATCH_KEY_UNSPECIFIED,
                MatchOperator.MATCH_OPERATOR_CONTAINS,
                "value"));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariablePlusOperatorString(
                MatchCategory.MATCH_CATEGORY_REQUEST,
                MatchKey.MATCH_KEY_HOST,
                MatchOperator.MATCH_OPERATOR_UNSPECIFIED,
                "value"));

    assertEquals(
        "REQUEST_URI_RAW \"@contains value\"",
        modsecRuleMappings.getVariablePlusOperatorString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            MatchKey.MATCH_KEY_URL,
            MatchOperator.MATCH_OPERATOR_CONTAINS,
            "value"));
    assertEquals(
        "REQUEST_URI_RAW \"!@contains value\"",
        modsecRuleMappings.getVariablePlusOperatorString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            MatchKey.MATCH_KEY_URL,
            MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
            "value"));

    // handling special match keys
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariablePlusOperatorString(
                MatchCategory.MATCH_CATEGORY_REQUEST,
                MatchKey.MATCH_KEY_HEADER_NAME,
                MatchOperator.MATCH_OPERATOR_LESS_THAN,
                "value"));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariablePlusOperatorString(
                MatchCategory.MATCH_CATEGORY_REQUEST,
                MatchKey.MATCH_KEY_PARAMETER_VALUE,
                MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
                "value"));
    assertEquals(
        "REQUEST_HEADERS_NAMES \"@streq value\"",
        modsecRuleMappings.getVariablePlusOperatorString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            MatchKey.MATCH_KEY_HEADER_NAME,
            MatchOperator.MATCH_OPERATOR_EQUALS,
            "value"));
    assertEquals(
        "&REQUEST_HEADERS_NAMES:value \"@eq 0\"",
        modsecRuleMappings.getVariablePlusOperatorString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            MatchKey.MATCH_KEY_HEADER_NAME,
            MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
            "value"));
    assertEquals(
        "ARGS \"@rx value\"",
        modsecRuleMappings.getVariablePlusOperatorString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            MatchKey.MATCH_KEY_PARAMETER_VALUE,
            MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
            "value"));
    assertEquals(
        "&ARGS:/value/ \"@eq 0\"",
        modsecRuleMappings.getVariablePlusOperatorString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            MatchKey.MATCH_KEY_PARAMETER_VALUE,
            MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
            "value"));
  }

  @Test
  public void test_getVariablePlusOperatorString_keyValueExpression() {

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariablePlusOperatorString(
                MatchCategory.MATCH_CATEGORY_REQUEST,
                KeyValueTag.KEY_VALUE_TAG_UNSPECIFIED,
                "key",
                MatchOperator.MATCH_OPERATOR_EQUALS,
                MatchOperator.MATCH_OPERATOR_EQUALS,
                "value"));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariablePlusOperatorString(
                MatchCategory.MATCH_CATEGORY_REQUEST,
                KeyValueTag.KEY_VALUE_TAG_HEADER,
                "key",
                MatchOperator.MATCH_OPERATOR_UNSPECIFIED,
                MatchOperator.MATCH_OPERATOR_EQUALS,
                "value"));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariablePlusOperatorString(
                MatchCategory.MATCH_CATEGORY_REQUEST,
                KeyValueTag.KEY_VALUE_TAG_HEADER,
                "key",
                MatchOperator.MATCH_OPERATOR_EQUALS,
                MatchOperator.MATCH_OPERATOR_UNSPECIFIED,
                "value"));

    assertEquals(
        "REQUEST_HEADERS:key \"@contains value\"",
        modsecRuleMappings.getVariablePlusOperatorString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            KeyValueTag.KEY_VALUE_TAG_HEADER,
            "key",
            MatchOperator.MATCH_OPERATOR_EQUALS,
            MatchOperator.MATCH_OPERATOR_CONTAINS,
            "value"));

    assertEquals(
        "REQUEST_HEADERS:key \"@contains \\\"value\\\":\\\"xyz\\\"\"",
        modsecRuleMappings.getVariablePlusOperatorString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            KeyValueTag.KEY_VALUE_TAG_HEADER,
            "key",
            MatchOperator.MATCH_OPERATOR_EQUALS,
            MatchOperator.MATCH_OPERATOR_CONTAINS,
            "\"value\":\"xyz\""));

    assertEquals(
        "REQUEST_HEADERS|!REQUEST_HEADERS:key \"!@rx value\"",
        modsecRuleMappings.getVariablePlusOperatorString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            KeyValueTag.KEY_VALUE_TAG_HEADER,
            "key",
            MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
            MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
            "value"));
  }

  @Test
  public void testGetModsecRule() {
    assertEquals(
        "SecRule HTTP_METHOD \"@streq post\" \"id=100001\"",
        modsecRuleMappings.getModsecRule("HTTP_METHOD \"@streq post\"", "\"id=100001\""));
  }
}
