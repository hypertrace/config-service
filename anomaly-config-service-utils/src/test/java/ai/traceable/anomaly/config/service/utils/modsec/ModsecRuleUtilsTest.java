package ai.traceable.anomaly.config.service.utils.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ModsecRuleUtilsTest {

  private final ModsecRuleUtils modsecRuleUtils = new ModsecRuleUtils();

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

  @Test
  public void testGetModsecCrsRuleIdNumber() {
    assertEquals(9123304, modsecRuleUtils.getModsecCrsRuleIdNumber("crs_9123304"));
    assertEquals(912, modsecRuleUtils.getModsecCrsRuleIdNumber("crs_912"));
  }
}
