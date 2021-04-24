package ai.traceable.customsignature.config.service.modsec.registry;

public class ModsecActions {
  private static final String COMMA_DELIMITER = ",";

  private static final String ID_FORMAT = "id:%d";
  private static final String PHASE_FORMAT = "phase:%d";
  private static final String MSG_FORMAT = "msg:'%s'";
  private static final String PARANOIA_LEVEL_TAG_FORMAT = "tag:'paranoia-level/%d'";
  private static final String RULE_UUID_TAG_FORMAT = "tag:'rule-uuid/%s'";

  private static final String DEFAULT_PHASE = String.format(PHASE_FORMAT, 2);
  private static final String DEFAULT_PARANOIA_LEVEL = String.format(PARANOIA_LEVEL_TAG_FORMAT, 1);

  private static final String CAPTURE = "capture";
  private static final String TRANSFORMATION_NONE = "t:none";
  private static final String LOG_DATA =
      "logdata:'Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}'";
  private static final String CUSTOM_SIGNATURE_TAG = "tag:'CUSTOM_SIGNATURE'";
  private static final String SEVERITY_CRITICAL = "severity:'CRITICAL'";
  private static final String CHAIN = "chain";

  private final long id;
  private final String msg;
  private final String ruleUuid;

  public ModsecActions(long id, String ruleUuid, String msg) {
    this.id = id;
    this.msg = msg;
    this.ruleUuid = ruleUuid;
  }

  public String getSingularRuleActionsString() {
    return "\""
        + String.join(
            COMMA_DELIMITER,
            String.format(ID_FORMAT, id),
            DEFAULT_PHASE,
            CAPTURE,
            TRANSFORMATION_NONE,
            String.format(MSG_FORMAT, msg),
            LOG_DATA,
            CUSTOM_SIGNATURE_TAG,
            DEFAULT_PARANOIA_LEVEL,
            String.format(RULE_UUID_TAG_FORMAT, ruleUuid),
            SEVERITY_CRITICAL)
        + "\"";
  }

  public String getChainedRulePrimaryActionsString() {
    return "\""
        + String.join(
            COMMA_DELIMITER,
            String.format(ID_FORMAT, id),
            DEFAULT_PHASE,
            CAPTURE,
            TRANSFORMATION_NONE,
            String.format(MSG_FORMAT, msg),
            LOG_DATA,
            CUSTOM_SIGNATURE_TAG,
            DEFAULT_PARANOIA_LEVEL,
            String.format(RULE_UUID_TAG_FORMAT, ruleUuid),
            SEVERITY_CRITICAL,
            CHAIN)
        + "\"";
  }

  public String getChainedRuleIntermediateActionsString() {
    return "\"" + String.join(COMMA_DELIMITER, CAPTURE, TRANSFORMATION_NONE, CHAIN) + "\"";
  }

  public String getChainedRuleFinalActionsString() {
    return "\"" + String.join(COMMA_DELIMITER, CAPTURE, TRANSFORMATION_NONE) + "\"";
  }
}
