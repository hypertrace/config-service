package ai.traceable.fraud.policy.config.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JexlExpressionUtilsTest {

  // --- escapeJexlString ---

  @Test
  void escapeJexlString_escapesSingleQuotes() {
    assertEquals("it\\'s a test", JexlExpressionUtils.escapeJexlString("it's a test"));
  }

  @Test
  void escapeJexlString_noChangeWhenNoQuotes() {
    assertEquals("clean", JexlExpressionUtils.escapeJexlString("clean"));
  }

  // --- valueToString ---

  @Test
  void valueToString_stringValue() {
    Value v = Value.newBuilder().setStringValue("hello").build();
    assertEquals("hello", JexlExpressionUtils.valueToString(v));
  }

  @Test
  void valueToString_integerNumber() {
    Value v = Value.newBuilder().setNumberValue(42.0).build();
    assertEquals("42", JexlExpressionUtils.valueToString(v));
  }

  @Test
  void valueToString_fractionalNumber() {
    Value v = Value.newBuilder().setNumberValue(3.14).build();
    assertEquals("3.14", JexlExpressionUtils.valueToString(v));
  }

  @Test
  void valueToString_boolValue() {
    Value v = Value.newBuilder().setBoolValue(true).build();
    assertEquals("true", JexlExpressionUtils.valueToString(v));
  }

  @Test
  void valueToString_nullValue() {
    Value v = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    assertEquals("null", JexlExpressionUtils.valueToString(v));
  }

  // --- toEqualsExpr ---

  @Test
  void toEqualsExpr_singleValue() {
    assertEquals(
        "$s.getEnv().equals('prod')",
        JexlExpressionUtils.toEqualsExpr("$s.getEnv()", List.of("prod")));
  }

  @Test
  void toEqualsExpr_multipleValues_orJoined() {
    String result = JexlExpressionUtils.toEqualsExpr("$s.getEnv()", List.of("prod", "staging"));
    assertEquals("($s.getEnv().equals('prod') || $s.getEnv().equals('staging'))", result);
  }

  @Test
  void toEqualsExpr_empty_returnsEmpty() {
    assertEquals("", JexlExpressionUtils.toEqualsExpr("$s.getEnv()", List.of()));
  }

  // --- toUrlRegexExpr ---

  @Test
  void toUrlRegexExpr_singleUrl() {
    assertEquals(
        "$s.getPath() =~ '/api/v1/.*'", JexlExpressionUtils.toUrlRegexExpr(Set.of("/api/v1/.*")));
  }

  // --- toChainedGetAccess ---

  @Test
  void toChainedGetAccess_standardPath() {
    assertEquals(
        ".get('inputData').get('Request').get('AuthToken')",
        JexlExpressionUtils.toChainedGetAccess("$.inputData.Request.AuthToken"));
  }

  @Test
  void toChainedGetAccess_simpleKey() {
    assertEquals(".get('key')", JexlExpressionUtils.toChainedGetAccess("$.key"));
  }

  @Test
  void toChainedGetAccess_leadingDot() {
    assertEquals(
        ".get('inputData').get('Request')",
        JexlExpressionUtils.toChainedGetAccess(".inputData.Request"));
  }

  @Test
  void toChainedGetAccess_barePath() {
    assertEquals(
        ".get('inputData').get('Request')",
        JexlExpressionUtils.toChainedGetAccess("inputData.Request"));
  }

  @Test
  void toChainedGetAccess_nullPath_throws() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> JexlExpressionUtils.toChainedGetAccess(null));
  }

  @Test
  void toChainedGetAccess_emptyPath_throws() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> JexlExpressionUtils.toChainedGetAccess(""));
  }

  @Test
  void toChainedGetAccess_dollarOnly_throws() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> JexlExpressionUtils.toChainedGetAccess("$"));
  }

  // --- toPascalCase ---

  @Test
  void toPascalCase_standard() {
    assertEquals("IpAddress", JexlExpressionUtils.toPascalCase("ip_address"));
  }

  @Test
  void toPascalCase_singleWord() {
    assertEquals("User", JexlExpressionUtils.toPascalCase("user"));
  }

  // --- toSnakeCase ---

  @Test
  void toSnakeCase_standard() {
    assertEquals("auth_token", JexlExpressionUtils.toSnakeCase("Auth Token"));
  }

  @Test
  void toSnakeCase_extraSpaces() {
    assertEquals("my_entity", JexlExpressionUtils.toSnakeCase("  My  Entity  "));
  }
}
