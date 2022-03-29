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
  public void testGetVariableString() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariableString(
                MatchCategory.MATCH_CATEGORY_REQUEST, MatchKey.MATCH_KEY_UNSPECIFIED));
    assertEquals(
        "REQUEST_URI_RAW",
        modsecRuleMappings.getVariableString(
            MatchCategory.MATCH_CATEGORY_REQUEST, MatchKey.MATCH_KEY_URL));

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariableString(
                MatchCategory.MATCH_CATEGORY_REQUEST,
                KeyValueTag.KEY_VALUE_TAG_UNSPECIFIED,
                "key",
                MatchOperator.MATCH_OPERATOR_EQUALS));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getVariableString(
                MatchCategory.MATCH_CATEGORY_REQUEST,
                KeyValueTag.KEY_VALUE_TAG_HEADER,
                "key",
                MatchOperator.MATCH_OPERATOR_UNSPECIFIED));
    assertEquals(
        "REQUEST_HEADERS:key",
        modsecRuleMappings.getVariableString(
            MatchCategory.MATCH_CATEGORY_REQUEST,
            KeyValueTag.KEY_VALUE_TAG_HEADER,
            "key",
            MatchOperator.MATCH_OPERATOR_EQUALS));
  }

  @Test
  public void testGetOperatorString() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            modsecRuleMappings.getOperatorString(
                MatchOperator.MATCH_OPERATOR_UNSPECIFIED, "value"));

    assertEquals(
        "\"@contains value\"",
        modsecRuleMappings.getOperatorString(MatchOperator.MATCH_OPERATOR_CONTAINS, "value"));
  }

  @Test
  public void testGetModsecRule() {
    assertEquals(
        "SecRule HTTP_METHOD \"streq post\" \"id=100001\"",
        modsecRuleMappings.getModsecRule("HTTP_METHOD", "\"streq post\"", "\"id=100001\""));
  }
}
