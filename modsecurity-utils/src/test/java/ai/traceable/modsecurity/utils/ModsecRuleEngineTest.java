package ai.traceable.modsecurity.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.condition.OS.LINUX;

import io.grpc.Status;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

public class ModsecRuleEngineTest {

  private static final String modsecRule =
      "SecRule ARGS \"@detectSQLi\" \"id:1,phase:1,deny,msg:'test rule',logdata:'matched',tag:'paranoia-level/4',tag:'rule-uuid/f461737e-9e95-11eb-a8b3-0242ac130003'\"";

  @Test
  @EnabledOnOs(LINUX)
  public void test_emptyConfig() {
    // test no crash
    assertEquals(Status.OK, ModsecRuleEngineUtils.validate(null));
  }

  @Test
  @EnabledOnOs(LINUX)
  public void test_invalidConfig() {
    // test no crash
    assertNotEquals(
        Status.OK,
        ModsecRuleEngineUtils.validate(
            "SecRule ARGS:a|b \"@streq c\" \"id:10000001,phase:2,t:none,msg:'invalid',logdata:'something',tag:'CUSTOM_SIGNATURE',tag:'paranoia-level/1'\""));
  }

  @Test
  @EnabledOnOs(LINUX)
  public void test_noAttributes() {
    assertEquals(Status.OK, ModsecRuleEngineUtils.validate(modsecRule));
    assertTrue(ModsecRuleEngineUtils.getModsecRuleMatches(modsecRule, null).isEmpty());
    assertTrue(
        ModsecRuleEngineUtils.getModsecRuleMatches(modsecRule, Collections.emptyMap()).isEmpty());
  }
}
