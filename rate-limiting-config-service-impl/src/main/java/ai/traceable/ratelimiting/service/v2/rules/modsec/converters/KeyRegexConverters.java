package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import java.util.List;
import java.util.function.Function;

public class KeyRegexConverters {
  private static final String OR_REGEX_DELIMITER = "|";
  private static final String DOT_CHARACTER_REGEX = "\\.";
  private static final String BEGINNING_REGEX_CHAR = "^";
  private static final String TERMINAL_REGEX_CHAR = "$";
  private static final String FILLER_REGEX = ".*";
  private static final String NON_DOT_FILLER_REGEX = "([^\\.])*";
  private static final Function<List<String>, String> MAKE_OR_REGEX_BLOCK =
      regexes -> "(" + String.join(OR_REGEX_DELIMITER, regexes) + ")";

  /**
   * A body param name http.request.body.x.y in modsec would appear as x.y and this should match
   * rules corresponding to both x and y
   */
  static String transformNestedParamNameRegex(
      String bodyParamNameString, boolean isRegex, boolean isLeaf) {
    if (bodyParamNameString.isBlank()) {
      return "";
    }

    if (isRegex) {
      if (bodyParamNameString.startsWith(BEGINNING_REGEX_CHAR)) {
        bodyParamNameString = bodyParamNameString.substring(1);
      } else {
        bodyParamNameString = NON_DOT_FILLER_REGEX + bodyParamNameString;
      }

      if (bodyParamNameString.endsWith(TERMINAL_REGEX_CHAR)) {
        bodyParamNameString = bodyParamNameString.substring(0, bodyParamNameString.length() - 1);
      } else {
        bodyParamNameString = bodyParamNameString + NON_DOT_FILLER_REGEX;
      }
    }

    String transformedRegex =
        MAKE_OR_REGEX_BLOCK.apply(List.of(DOT_CHARACTER_REGEX, BEGINNING_REGEX_CHAR))
            + bodyParamNameString;
    if (isLeaf) {
      return transformedRegex + TERMINAL_REGEX_CHAR;
    } else {
      return transformedRegex
          + MAKE_OR_REGEX_BLOCK.apply(List.of(DOT_CHARACTER_REGEX, TERMINAL_REGEX_CHAR));
    }
  }

  /** A key regex inside blob should not have anchor endpoints */
  static String generateBlobRegexFromKeyPatterns(String keyRegex) {
    return removeAnchorEndpoints(keyRegex);
  }

  /**
   * A key-value regex inside blob should not have anchor endpoints and should support both
   * combinations
   */
  static String generateBlobRegexFromKeyValuePatterns(String keyRegex, String valueRegex) {
    keyRegex = removeAnchorEndpoints(keyRegex);
    valueRegex = removeAnchorEndpoints(valueRegex);
    return keyRegex
        + FILLER_REGEX
        + valueRegex
        + OR_REGEX_DELIMITER
        + valueRegex
        + FILLER_REGEX
        + keyRegex;
  }

  private static String removeAnchorEndpoints(String keyRegex) {
    if (keyRegex.isBlank()) {
      return "";
    }

    if (keyRegex.startsWith(BEGINNING_REGEX_CHAR)) {
      keyRegex = keyRegex.substring(1);
    }

    if (keyRegex.endsWith(TERMINAL_REGEX_CHAR)) {
      keyRegex = keyRegex.substring(0, keyRegex.length() - 1);
    }

    return keyRegex;
  }
}
