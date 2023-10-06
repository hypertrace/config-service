package ai.traceable.modsecurity.rule.secrule.actions;

import java.util.Optional;

public class ModsecActions {
  private static final String COMMA_DELIMITER = ",";

  private static final String ID_FORMAT = "id:%d";
  private static final String PHASE_FORMAT = "phase:%d";
  private static final String MSG_FORMAT = "msg:'%s'";
  private static final String LOG_DATA_FORMAT = "logdata:'%s'";
  private static final String PARANOIA_LEVEL_TAG_FORMAT = "tag:'paranoia-level/%d'";
  private static final String RULE_UUID_TAG_FORMAT = "tag:'rule-uuid/%s'";

  private static final String DEFAULT_PHASE = String.format(PHASE_FORMAT, 2);
  private static final String RESPONSE_PHASE = String.format(PHASE_FORMAT, 4);
  private static final String DEFAULT_PARANOIA_LEVEL = String.format(PARANOIA_LEVEL_TAG_FORMAT, 1);
  private static final String DEFAULT_LOG_DATA =
      "Matched Data: %{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}";
  private static final String CAPTURE = "capture";
  private static final String BLOCK = "block";
  private static final String TRANSFORMATION_NONE = "t:none";
  private static final String CUSTOM_SIGNATURE_TAG = "tag:'CUSTOM_SIGNATURE'";
  private static final String SEVERITY_CRITICAL = "severity:'CRITICAL'";
  private static final String CHAIN = "chain";

  private final long id;
  private final String msg;
  private final String ruleUuid;
  private final String logData;

  public ModsecActions(long id, String ruleUuid, String msg, Optional<String> logData) {
    this.id = id;
    this.msg = msg;
    this.ruleUuid = ruleUuid;
    this.logData = logData.orElse(DEFAULT_LOG_DATA);
  }

  public String getActionsString(ModsecActionsType actionsType, boolean responsePhase) {
    switch (actionsType) {
      case SINGULAR:
        return getSingularRuleActionsString(responsePhase);
      case CHAINED_PRIMARY:
        return getChainedRulePrimaryActionsString(responsePhase);
      case CHAINED_INTERMEDIATE:
        return getChainedRuleIntermediateActionsString();
      case CHAINED_FINAL:
        return getChainedRuleFinalActionsString();
      default:
        throw new IllegalArgumentException("Unsupported actionsType: " + actionsType);
    }
  }

  private String getSingularRuleActionsString(boolean responsePhase) {
    return "\""
        + String.join(
            COMMA_DELIMITER,
            String.format(ID_FORMAT, id),
            responsePhase ? RESPONSE_PHASE : DEFAULT_PHASE,
            CAPTURE,
            BLOCK,
            TRANSFORMATION_NONE,
            String.format(MSG_FORMAT, msg),
            String.format(LOG_DATA_FORMAT, logData),
            CUSTOM_SIGNATURE_TAG,
            DEFAULT_PARANOIA_LEVEL,
            String.format(RULE_UUID_TAG_FORMAT, ruleUuid),
            SEVERITY_CRITICAL)
        + "\"";
  }

  private String getChainedRulePrimaryActionsString(boolean responsePhase) {
    return "\""
        + String.join(
            COMMA_DELIMITER,
            String.format(ID_FORMAT, id),
            responsePhase ? RESPONSE_PHASE : DEFAULT_PHASE,
            CAPTURE,
            TRANSFORMATION_NONE,
            String.format(MSG_FORMAT, msg),
            String.format(LOG_DATA_FORMAT, logData),
            CUSTOM_SIGNATURE_TAG,
            DEFAULT_PARANOIA_LEVEL,
            String.format(RULE_UUID_TAG_FORMAT, ruleUuid),
            SEVERITY_CRITICAL,
            CHAIN)
        + "\"";
  }

  private String getChainedRuleIntermediateActionsString() {
    return "\"" + String.join(COMMA_DELIMITER, CAPTURE, TRANSFORMATION_NONE, CHAIN) + "\"";
  }

  private String getChainedRuleFinalActionsString() {
    return "\"" + String.join(COMMA_DELIMITER, CAPTURE, BLOCK, TRANSFORMATION_NONE) + "\"";
  }
}
