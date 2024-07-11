package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class KeyRegexConvertersTest {
  @Test
  void testBodyParamNameTransformer() {
    assertEquals(
        "^(.*?[.])?abc([.].*?)?$",
        KeyRegexConverters.transformNestedParamNameRegex("abc", false, false));
    assertEquals(
        "^(.*?[.])?abc$", KeyRegexConverters.transformNestedParamNameRegex("abc", false, true));
    assertEquals("abc", KeyRegexConverters.transformNestedParamNameRegex("abc", true, false));
    assertEquals("abc[^.]*?$", KeyRegexConverters.transformNestedParamNameRegex("abc", true, true));
    assertEquals(
        "^(.*?[.])?abc([.].*?)?$",
        KeyRegexConverters.transformNestedParamNameRegex("^abc$", true, false));
    assertEquals(
        "^(.*?[.])?abc$", KeyRegexConverters.transformNestedParamNameRegex("^abc$", true, true));
  }

  @Test
  void testKeyRegexForBlob() {
    assertEquals("abc", KeyRegexConverters.generateBlobRegexFromKeyPatterns("^abc$"));
  }

  @Test
  void testKeyValueRegexForBlob() {
    assertEquals(
        "abc.*?xyz|xyz.*?abc",
        KeyRegexConverters.generateBlobRegexFromKeyValuePatterns("^abc$", "^xyz$"));
  }
}
