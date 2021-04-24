package ai.traceable.customsignature.config.service.modsec.registry;

import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;

public class ModsecRuleMappings {
  private static final String SEC_RULE = "SecRule";
  private static final String COLON_DELIMITER = ":";
  private static final String PIPE_DELIMITER = "|";
  private static final String SPACE_DELIMITER = " ";
  private static final String NOT_PREFIX = "!";
  private static final String FRONT_SLASH_WRAPPER = "/";

  private static final String HOST_HEADER = "Host";
  private static final String X_FORWARDED_HOST_HEADER = "x-forwarded-host";
  private static final String FORWARDED_HEADER = "forwarded";
  private static final String USER_AGENT_HEADER = "User-Agent";

  private final Map<MatchKey, String> matchKeyMappings = new HashMap<>();
  private final Map<MatchOperator, String> matchOperatorMappings = new HashMap<>();
  private final Map<KeyValueTag, String> keyValueTagMappings = new HashMap<>();

  @Inject
  public ModsecRuleMappings() {
    initMatchKeyMappings();
    initMatchOperatorMappings();
    initKeyValueTagMappings();
  }

  public String getModsecRule(String variable, String operator, String actions) {
    return String.join(SPACE_DELIMITER, SEC_RULE, variable, operator, actions);
  }

  public String getVariableString(MatchKey matchKey) {
    if (!matchKeyMappings.containsKey(matchKey)) {
      throw new UnsupportedOperationException(
          String.format("Cannot translate unknown match key '%s'", matchKey));
    }
    return matchKeyMappings.get(matchKey);
  }

  public String getVariableString(
      KeyValueTag keyValueTag, String key, MatchOperator keyMatchOperator) {
    if (!keyValueTagMappings.containsKey(keyValueTag)) {
      throw new UnsupportedOperationException(
          String.format("Cannot translate unknown key-value tag '%s'", keyValueTag));
    }
    switch (keyMatchOperator) {
      case MATCH_OPERATOR_EQUALS:
        return keyValueTagMappings.get(keyValueTag) + COLON_DELIMITER + key;
      case MATCH_OPERATOR_NOT_EQUAL:
        return keyValueTagMappings.get(keyValueTag)
            + PIPE_DELIMITER
            + NOT_PREFIX
            + keyValueTagMappings.get(keyValueTag)
            + COLON_DELIMITER
            + key;
      case MATCH_OPERATOR_MATCHES_REGEX:
        return keyValueTagMappings.get(keyValueTag)
            + COLON_DELIMITER
            + getWrappedString(key, FRONT_SLASH_WRAPPER);
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return keyValueTagMappings.get(keyValueTag)
            + PIPE_DELIMITER
            + NOT_PREFIX
            + keyValueTagMappings.get(keyValueTag)
            + COLON_DELIMITER
            + getWrappedString(key, FRONT_SLASH_WRAPPER);
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot translate unknown key-match operator '%s'", keyMatchOperator));
    }
  }

  public String getOperatorString(MatchOperator matchOperator, String value) {
    if (!matchOperatorMappings.containsKey(matchOperator)) {
      throw new UnsupportedOperationException(
          String.format("Cannot translate unknown match operator '%s'", matchOperator));
    }
    return getWrappedString(
        matchOperatorMappings.get(matchOperator) + SPACE_DELIMITER + value, "\"");
  }

  private String getWrappedString(String str, String wrapper) {
    return wrapper + str + wrapper;
  }

  private void initMatchKeyMappings() {
    matchKeyMappings.put(MatchKey.MATCH_KEY_URL, ModsecVariables.REQUEST_URI_RAW.name());
    matchKeyMappings.put(
        MatchKey.MATCH_KEY_HOST,
        ModsecVariables.REQUEST_HEADERS.name()
            + COLON_DELIMITER
            + HOST_HEADER
            + PIPE_DELIMITER
            + ModsecVariables.REQUEST_HEADERS.name()
            + COLON_DELIMITER
            + X_FORWARDED_HOST_HEADER
            + PIPE_DELIMITER
            + ModsecVariables.REQUEST_HEADERS.name()
            + COLON_DELIMITER
            + FORWARDED_HEADER);
    matchKeyMappings.put(MatchKey.MATCH_KEY_HTTP_METHOD, ModsecVariables.REQUEST_METHOD.name());
    matchKeyMappings.put(
        MatchKey.MATCH_KEY_USER_AGENT,
        ModsecVariables.REQUEST_HEADERS.name() + COLON_DELIMITER + USER_AGENT_HEADER);
    matchKeyMappings.put(
        MatchKey.MATCH_KEY_HEADER_NAME, ModsecVariables.REQUEST_HEADERS_NAMES.name());
    matchKeyMappings.put(MatchKey.MATCH_KEY_HEADER_VALUE, ModsecVariables.REQUEST_HEADERS.name());
    matchKeyMappings.put(MatchKey.MATCH_KEY_PARAMETER_NAME, ModsecVariables.ARGS_NAMES.name());
    matchKeyMappings.put(MatchKey.MATCH_KEY_PARAMETER_VALUE, ModsecVariables.ARGS.name());
  }

  private void initMatchOperatorMappings() {
    matchOperatorMappings.put(
        MatchOperator.MATCH_OPERATOR_EQUALS, ModsecOperators.EQUALS.getOperator());
    matchOperatorMappings.put(
        MatchOperator.MATCH_OPERATOR_NOT_EQUAL, ModsecOperators.NOT_EQUAL.getOperator());
    matchOperatorMappings.put(
        MatchOperator.MATCH_OPERATOR_MATCHES_REGEX, ModsecOperators.MATCHES_REGEX.getOperator());
    matchOperatorMappings.put(
        MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
        ModsecOperators.NOT_MATCH_REGEX.getOperator());
    matchOperatorMappings.put(
        MatchOperator.MATCH_OPERATOR_GREATER_THAN, ModsecOperators.GREATER_THAN.getOperator());
    matchOperatorMappings.put(
        MatchOperator.MATCH_OPERATOR_LESS_THAN, ModsecOperators.LESS_THAN.getOperator());
    matchOperatorMappings.put(
        MatchOperator.MATCH_OPERATOR_CONTAINS, ModsecOperators.CONTAINS.getOperator());
    matchOperatorMappings.put(
        MatchOperator.MATCH_OPERATOR_NOT_CONTAIN, ModsecOperators.NOT_CONTAIN.getOperator());
  }

  private void initKeyValueTagMappings() {
    keyValueTagMappings.put(
        KeyValueTag.KEY_VALUE_TAG_HEADER, ModsecVariables.REQUEST_HEADERS.name());
    keyValueTagMappings.put(KeyValueTag.KEY_VALUE_TAG_PARAMETER, ModsecVariables.ARGS.name());
  }
}
