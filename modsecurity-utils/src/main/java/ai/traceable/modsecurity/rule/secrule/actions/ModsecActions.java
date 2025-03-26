package ai.traceable.modsecurity.rule.secrule.actions;

import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SEC_RULE_ID_REGEX;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ModsecActions {
  private static final String COMMA_DELIMITER = ",";
  private static final String EMPTY_STRING = "";
  private static final String QUOTE = "\"";
  private static final String ID_ACTION_PREFIX = "\"id:";
  private static final String ID_FORMAT = "id:%d";
  private static final String PHASE_FORMAT = "phase:%d";
  private static final String MSG_FORMAT = "msg:'%s'";
  private static final String LOG_DATA_FORMAT = "logdata:'%s'";
  private static final String PARANOIA_LEVEL_TAG_FORMAT = "tag:'paranoia-level/%d'";
  private static final String RULE_UUID_TAG_FORMAT = "tag:'rule-uuid/%s'";
  private static final Pattern MESSAGE_REGEX =
      Pattern.compile(
          "msg[ \\t\\x0B\\f\\r]{0,10}:[ \\t\\x0B\\f\\r]{0,10}'.*?'[ \\t\\x0B\\f\\r]{0,10}");
  private static final Pattern LOG_DATA_REGEX =
      Pattern.compile(
          "logdata[ \\t\\x0B\\f\\r]{0,10}:[ \\t\\x0B\\f\\r]{0,10}'.*?'[ \\t\\x0B\\f\\r]{0,10}");

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
  private final String phase;

  public ModsecActions(
      long id, String ruleUuid, String msg, Optional<String> logData, boolean responsePhase) {
    this.id = id;
    this.msg = msg;
    this.ruleUuid = ruleUuid;
    this.logData = logData.orElse(DEFAULT_LOG_DATA);
    this.phase = responsePhase ? RESPONSE_PHASE : DEFAULT_PHASE;
  }

  public String getActionsString(ModsecActionsType actionsType) {
    switch (actionsType) {
      case SINGULAR:
        return getSingularRuleActionsString();
      case CHAINED_PRIMARY:
        return getChainedRulePrimaryActionsString();
      case CHAINED_INTERMEDIATE:
        return getChainedRuleIntermediateActionsString();
      case CHAINED_FINAL:
        return getChainedRuleFinalActionsString();
      default:
        throw new IllegalArgumentException("Unsupported actionsType: " + actionsType);
    }
  }

  public String modifyActionsStringInSecRule(String inputSecRule, ModsecActionsType actionsType) {
    int modsecIdActionStartIndex = inputSecRule.indexOf(ID_ACTION_PREFIX);
    if (modsecIdActionStartIndex == -1) {
      return inputSecRule;
    }
    String secRuleSubStringFromIdAction = inputSecRule.substring(modsecIdActionStartIndex);
    switch (actionsType) {
      case SINGULAR:
        secRuleSubStringFromIdAction = modifyActionsString(secRuleSubStringFromIdAction);
        break;
      case CHAINED_PRIMARY:
        secRuleSubStringFromIdAction = modifyActionsString(secRuleSubStringFromIdAction);
        secRuleSubStringFromIdAction = addChainAction(secRuleSubStringFromIdAction);
        break;
      case CHAINED_INTERMEDIATE:
        secRuleSubStringFromIdAction =
            replaceSecRuleIdAction(secRuleSubStringFromIdAction, EMPTY_STRING);
        secRuleSubStringFromIdAction = addChainAction(secRuleSubStringFromIdAction);
        break;
      case CHAINED_FINAL:
        secRuleSubStringFromIdAction =
            replaceSecRuleIdAction(secRuleSubStringFromIdAction, EMPTY_STRING);
        break;
      default:
        throw new IllegalArgumentException("Unsupported actionsType: " + actionsType);
    }
    return inputSecRule.substring(0, modsecIdActionStartIndex) + secRuleSubStringFromIdAction;
  }

  private String getSingularRuleActionsString() {
    return "\""
        + String.join(
            COMMA_DELIMITER,
            String.format(ID_FORMAT, id),
            phase,
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

  private String getChainedRulePrimaryActionsString() {
    return "\""
        + String.join(
            COMMA_DELIMITER,
            String.format(ID_FORMAT, id),
            phase,
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

  private String modifyActionsString(String actionsString) {
    String modifiedActionsString =
        replaceSecRuleIdAction(actionsString, String.format(ID_FORMAT, id) + COMMA_DELIMITER);
    modifiedActionsString = replaceMsgAction(modifiedActionsString);
    modifiedActionsString = addLogActionIfAbsent(modifiedActionsString);
    modifiedActionsString = addRuleUuidTagAction(modifiedActionsString);
    return modifiedActionsString;
  }

  private String replaceSecRuleIdAction(String actionsString, String value) {
    Matcher matcher = SEC_RULE_ID_REGEX.matcher(actionsString);
    return matcher.replaceFirst(value);
  }

  private String replaceMsgAction(String actionsString) {
    Matcher matcher = MESSAGE_REGEX.matcher(actionsString);
    String msgAction = String.format(MSG_FORMAT, msg);
    if (matcher.find()) {
      return actionsString.substring(0, matcher.start())
          + msgAction
          + actionsString.substring(matcher.end());
    }
    return QUOTE + msgAction + COMMA_DELIMITER + actionsString.substring(1);
  }

  private String addLogActionIfAbsent(String actionsString) {
    Matcher matcher = LOG_DATA_REGEX.matcher(actionsString);
    if (!matcher.find()) {
      return QUOTE
          + String.format(LOG_DATA_FORMAT, logData)
          + COMMA_DELIMITER
          + actionsString.substring(1);
    }
    return actionsString;
  }

  private String addRuleUuidTagAction(String actionString) {
    return QUOTE
        + String.format(RULE_UUID_TAG_FORMAT, ruleUuid)
        + COMMA_DELIMITER
        + actionString.substring(1);
  }

  private String addChainAction(String actionString) {
    return actionString.substring(0, actionString.lastIndexOf(QUOTE))
        + COMMA_DELIMITER
        + CHAIN
        + QUOTE;
  }
}
