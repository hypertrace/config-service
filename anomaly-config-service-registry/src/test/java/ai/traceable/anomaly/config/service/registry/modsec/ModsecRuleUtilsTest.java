package ai.traceable.anomaly.config.service.registry.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ModsecRuleUtilsTest {

  private final ModsecRuleUtils modsecRuleUtils = new ModsecRuleUtils();
  public static final String MODSEC_RULE_PREFIX = "crs_";
  public static final int MODSEC_RULE_PREFIX_LENGTH = MODSEC_RULE_PREFIX.length();
  // standard crs rules have crs_### as the parent identifier -- hence using 3 digits
  public static final int MODSEC_PARENT_RULE_ID_LENGTH = 3 + MODSEC_RULE_PREFIX_LENGTH;

  @Test
  public void testGetModsecRuleId() {
    assertEquals("crs_9123304", modsecRuleUtils.getModsecRuleId(9123304));
    assertEquals("crs_912334", modsecRuleUtils.getModsecRuleId(912334));
    assertEquals("crs_912", modsecRuleUtils.getModsecRuleId(912));
  }

  @Test
  public void testGetModsecParentRuleId() {
    assertEquals("crs_912", modsecRuleUtils.getModsecParentRuleId("crs_9123304"));
    assertEquals("crs_912", modsecRuleUtils.getModsecParentRuleId("crs_912330"));
    assertEquals("crs_912", modsecRuleUtils.getModsecParentRuleId("crs_912"));
  }
}
