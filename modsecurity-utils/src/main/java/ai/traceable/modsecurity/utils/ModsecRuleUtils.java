package ai.traceable.modsecurity.utils;

public class ModsecRuleUtils {

  public static final String MODSEC_RULE_PREFIX = "crs_";
  public static final int MODSEC_RULE_PREFIX_LENGTH = MODSEC_RULE_PREFIX.length();
  // standard crs rules have crs_### as the parent identifier -- hence using 3 digits
  public static final int MODSEC_PARENT_RULE_ID_LENGTH = 3 + MODSEC_RULE_PREFIX_LENGTH;

  public String getModsecRuleId(long ruleIdNumber) {
    return MODSEC_RULE_PREFIX + ruleIdNumber;
  }

  public long getModsecCrsRuleIdNumber(String modsecRuleId) {
    return Long.parseLong(modsecRuleId.substring(MODSEC_RULE_PREFIX_LENGTH));
  }

  public boolean isValidRuleId(String ruleId) {
    return ruleId.startsWith(MODSEC_RULE_PREFIX);
  }

  public boolean isValidSubRuleId(String subRuleId, String ruleId) {
    return subRuleId.startsWith(ruleId);
  }

  public String getModsecParentRuleId(String modsecRuleId) {
    return modsecRuleId.substring(0, MODSEC_PARENT_RULE_ID_LENGTH);
  }
}
