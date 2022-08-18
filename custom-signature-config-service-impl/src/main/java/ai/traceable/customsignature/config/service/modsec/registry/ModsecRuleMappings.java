package ai.traceable.customsignature.config.service.modsec.registry;

import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;

public class ModsecRuleMappings {
  private static final String SEC_RULE = "SecRule";
  private static final String AMPERSAND_PREFIX = "&";
  private static final String COLON_DELIMITER = ":";
  private static final String PIPE_DELIMITER = "|";
  private static final String SPACE_DELIMITER = " ";
  private static final String NOT_PREFIX = "!";
  private static final String FRONT_SLASH_WRAPPER = "/";
  private static final String EQUALS_ZERO_OPERATOR_STRING = "\"@eq 0\"";

  private static final String HOST_HEADER = "Host";
  private static final String X_FORWARDED_HOST_HEADER = "x-forwarded-host";
  private static final String FORWARDED_HEADER = "forwarded";
  private static final String USER_AGENT_HEADER = "User-Agent";

  private static final Set<MatchKey> specialMatchKeySet =
      Set.of(
          MatchKey.MATCH_KEY_HEADER_NAME,
          MatchKey.MATCH_KEY_HEADER_VALUE,
          MatchKey.MATCH_KEY_PARAMETER_NAME,
          MatchKey.MATCH_KEY_PARAMETER_VALUE);
  private static final Set<MatchOperator> specialNotAllowedMatchOperatorSet =
      Set.of(
          MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
          MatchOperator.MATCH_OPERATOR_GREATER_THAN,
          MatchOperator.MATCH_OPERATOR_LESS_THAN);
  private static final Set<MatchOperator> escapeQuotesMatchOperatorSet =
      Set.of(
          MatchOperator.MATCH_OPERATOR_EQUALS,
          MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
          MatchOperator.MATCH_OPERATOR_CONTAINS,
          MatchOperator.MATCH_OPERATOR_NOT_CONTAIN);

  private final Map<MatchCategory, Map<MatchKey, String>> matchKeyMappings = new HashMap<>();
  private final Map<MatchOperator, String> matchOperatorMappings = new HashMap<>();
  private final Map<MatchCategory, Map<KeyValueTag, String>> keyValueTagMappings = new HashMap<>();

  @Inject
  public ModsecRuleMappings() {
    initMatchKeyMappings();
    initMatchOperatorMappings();
    initKeyValueTagMappings();
  }

  public String getModsecRule(String variablePlusOperatorString, String actions) {
    return String.join(SPACE_DELIMITER, SEC_RULE, variablePlusOperatorString, actions);
  }

  public String getVariablePlusOperatorString(
      MatchCategory matchCategory,
      KeyValueTag keyValueTag,
      String key,
      MatchOperator keyMatchOperator,
      MatchOperator valueMatchOperator,
      String value) {
    // MATCH_CATEGORY_UNSPECIFIED resolves to MATCH_CATEGORY_REQUEST for backward compatibility
    if (matchCategory.equals(MatchCategory.MATCH_CATEGORY_UNSPECIFIED)) {
      matchCategory = MatchCategory.MATCH_CATEGORY_REQUEST;
    }
    if (!(keyValueTagMappings.containsKey(matchCategory)
        && keyValueTagMappings.get(matchCategory).containsKey(keyValueTag))) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot translate unknown match category, key-value tag '%s', '%s'",
              matchCategory, keyValueTag));
    }
    String variableString = keyValueTagMappings.get(matchCategory).get(keyValueTag);
    switch (keyMatchOperator) {
      case MATCH_OPERATOR_EQUALS:
        variableString += COLON_DELIMITER + key;
        break;
      case MATCH_OPERATOR_NOT_EQUAL:
        variableString += PIPE_DELIMITER + NOT_PREFIX + variableString + COLON_DELIMITER + key;
        break;
      case MATCH_OPERATOR_MATCHES_REGEX:
        variableString += COLON_DELIMITER + getWrappedString(key, FRONT_SLASH_WRAPPER);
        break;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        variableString +=
            PIPE_DELIMITER
                + NOT_PREFIX
                + variableString
                + COLON_DELIMITER
                + getWrappedString(key, FRONT_SLASH_WRAPPER);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot translate unknown key-match operator '%s'", keyMatchOperator));
    }
    return String.join(
        SPACE_DELIMITER, variableString, getOperatorString(valueMatchOperator, value));
  }

  public String getVariablePlusOperatorString(
      MatchCategory matchCategory, MatchKey matchKey, MatchOperator matchOperator, String value) {
    if (specialMatchKeySet.contains(matchKey)) {
      if (specialNotAllowedMatchOperatorSet.contains(matchOperator)) {
        throw new UnsupportedOperationException(
            String.format(
                "Match operator %s is not supported for match key '%s'", matchOperator, matchKey));
      }
      if (matchOperator == MatchOperator.MATCH_OPERATOR_NOT_EQUAL
          || matchOperator == MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX) {
        String valueString =
            matchOperator == MatchOperator.MATCH_OPERATOR_NOT_EQUAL
                ? value
                : getWrappedString(value, FRONT_SLASH_WRAPPER);
        String variableString =
            AMPERSAND_PREFIX
                + getVariableString(matchCategory, matchKey)
                + COLON_DELIMITER
                + valueString;
        return String.join(SPACE_DELIMITER, variableString, EQUALS_ZERO_OPERATOR_STRING);
      }
    }
    return String.join(
        SPACE_DELIMITER,
        getVariableString(matchCategory, matchKey),
        getOperatorString(matchOperator, value));
  }

  private String getVariableString(MatchCategory matchCategory, MatchKey matchKey) {
    // MATCH_CATEGORY_UNSPECIFIED resolves to MATCH_CATEGORY_REQUEST for backward compatibility
    if (matchCategory.equals(MatchCategory.MATCH_CATEGORY_UNSPECIFIED)) {
      matchCategory = MatchCategory.MATCH_CATEGORY_REQUEST;
    }
    if (!(matchKeyMappings.containsKey(matchCategory)
        && matchKeyMappings.get(matchCategory).containsKey(matchKey))) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot translate unknown match category, match key '%s', '%s'",
              matchCategory, matchKey));
    }
    return matchKeyMappings.get(matchCategory).get(matchKey);
  }

  private String getOperatorString(MatchOperator matchOperator, String value) {
    if (!matchOperatorMappings.containsKey(matchOperator)) {
      throw new UnsupportedOperationException(
          String.format("Cannot translate unknown match operator '%s'", matchOperator));
    }
    if (escapeQuotesMatchOperatorSet.contains(matchOperator)) {
      // escape quotes to form the correct modsec rule syntax
      value = value.replace("\"", "\\\"");
    }
    return getWrappedString(
        matchOperatorMappings.get(matchOperator) + SPACE_DELIMITER + value, "\"");
  }

  private String getWrappedString(String str, String wrapper) {
    return wrapper + str + wrapper;
  }

  private void initMatchKeyMappings() {
    Map<MatchKey, String> requestMappings = new HashMap<>();
    requestMappings.put(MatchKey.MATCH_KEY_URL, ModsecVariables.REQUEST_URI_RAW.name());
    requestMappings.put(
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
    requestMappings.put(MatchKey.MATCH_KEY_HTTP_METHOD, ModsecVariables.REQUEST_METHOD.name());
    requestMappings.put(
        MatchKey.MATCH_KEY_USER_AGENT,
        ModsecVariables.REQUEST_HEADERS.name() + COLON_DELIMITER + USER_AGENT_HEADER);
    requestMappings.put(
        MatchKey.MATCH_KEY_HEADER_NAME, ModsecVariables.REQUEST_HEADERS_NAMES.name());
    requestMappings.put(MatchKey.MATCH_KEY_HEADER_VALUE, ModsecVariables.REQUEST_HEADERS.name());
    requestMappings.put(MatchKey.MATCH_KEY_BODY, ModsecVariables.REQUEST_BODY.name());
    requestMappings.put(MatchKey.MATCH_KEY_PARAMETER_NAME, ModsecVariables.ARGS_NAMES.name());
    requestMappings.put(MatchKey.MATCH_KEY_PARAMETER_VALUE, ModsecVariables.ARGS.name());

    Map<MatchKey, String> responseMappings = new HashMap<>();
    responseMappings.put(MatchKey.MATCH_KEY_STATUS_CODE, ModsecVariables.RESPONSE_STATUS.name());
    responseMappings.put(
        MatchKey.MATCH_KEY_HEADER_NAME, ModsecVariables.RESPONSE_HEADERS_NAMES.name());
    responseMappings.put(MatchKey.MATCH_KEY_HEADER_VALUE, ModsecVariables.RESPONSE_HEADERS.name());
    responseMappings.put(MatchKey.MATCH_KEY_BODY, ModsecVariables.RESPONSE_BODY.name());

    matchKeyMappings.put(MatchCategory.MATCH_CATEGORY_REQUEST, requestMappings);
    matchKeyMappings.put(MatchCategory.MATCH_CATEGORY_RESPONSE, responseMappings);
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
    Map<KeyValueTag, String> requestMappings = new HashMap<>();
    requestMappings.put(KeyValueTag.KEY_VALUE_TAG_HEADER, ModsecVariables.REQUEST_HEADERS.name());
    requestMappings.put(KeyValueTag.KEY_VALUE_TAG_PARAMETER, ModsecVariables.ARGS.name());

    Map<KeyValueTag, String> responseMappings = new HashMap<>();
    responseMappings.put(KeyValueTag.KEY_VALUE_TAG_HEADER, ModsecVariables.RESPONSE_HEADERS.name());

    keyValueTagMappings.put(MatchCategory.MATCH_CATEGORY_REQUEST, requestMappings);
    keyValueTagMappings.put(MatchCategory.MATCH_CATEGORY_RESPONSE, responseMappings);
  }
}
