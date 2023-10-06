package ai.traceable.modsecurity.rule.secrule.variables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class ModsecVariableTest {

  @Test
  public void test_exception() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ModsecVariable(
                ModsecVariableMetadata.REQUEST_HEADERS, ModsecVariableMetadataOperator.NOT, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ModsecVariable(
                ModsecVariableMetadata.REQUEST_HEADERS,
                ModsecVariableMetadataOperator.NOT,
                new ModsecVariableMetadataKey(
                    ModsecVariableMetadataKey.ModsecVariableKeyOperator.MATCHES_REGEX,
                    "x-(real|forward)")));
  }

  @Test
  public void test_toString() {
    assertEquals(
        "REQUEST_HEADERS", new ModsecVariable(ModsecVariableMetadata.REQUEST_HEADERS).toString());
    assertEquals(
        "&ARGS_NAMES",
        new ModsecVariable(
                ModsecVariableMetadata.ARGS_NAMES, ModsecVariableMetadataOperator.COUNT, null)
            .toString());
    assertEquals(
        "!REQUEST_HEADERS:x-real-ip",
        new ModsecVariable(
                ModsecVariableMetadata.REQUEST_HEADERS,
                ModsecVariableMetadataOperator.NOT,
                new ModsecVariableMetadataKey(
                    ModsecVariableMetadataKey.ModsecVariableKeyOperator.EQUALS, "x-real-ip"))
            .toString());
    assertEquals(
        "!REQUEST_HEADERS:/x-real/",
        new ModsecVariable(
                ModsecVariableMetadata.REQUEST_HEADERS,
                ModsecVariableMetadataOperator.NOT,
                new ModsecVariableMetadataKey(
                    ModsecVariableMetadataKey.ModsecVariableKeyOperator.MATCHES_REGEX, "x-real"))
            .toString());
  }
}
