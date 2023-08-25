package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import java.util.function.Function;

public class KeyRegexConverters {
  private static final String OR_REGEX_DELIMITER = "|";
  private static final String DOT_CHARACTER_REGEX = "[.]";
  private static final String BEGINNING_REGEX_CHAR = "^";
  private static final String TERMINAL_REGEX_CHAR = "$";
  private static final String FILLER_REGEX = ".*";
  private static final String NON_DOT_FILLER_REGEX = "[^.]*";
  private static final Function<String, String> MAKE_OPTIONAL_BLOCK = regex -> "(" + regex + ")?";

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
      boolean startsWithAnchor = bodyParamNameString.startsWith(BEGINNING_REGEX_CHAR);
      boolean endsWithAnchor = bodyParamNameString.endsWith(TERMINAL_REGEX_CHAR);
      if (!startsWithAnchor && !endsWithAnchor) {
        if (isLeaf) {
          return bodyParamNameString + NON_DOT_FILLER_REGEX + TERMINAL_REGEX_CHAR;
        }
        return bodyParamNameString;
      }

      if (startsWithAnchor) {
        bodyParamNameString = bodyParamNameString.substring(1);
      }

      if (endsWithAnchor) {
        bodyParamNameString = bodyParamNameString.substring(0, bodyParamNameString.length() - 1);
      }
    }

    String transformedRegex =
        BEGINNING_REGEX_CHAR
            + MAKE_OPTIONAL_BLOCK.apply(FILLER_REGEX + DOT_CHARACTER_REGEX)
            + bodyParamNameString;

    if (isLeaf) {
      return transformedRegex + TERMINAL_REGEX_CHAR;
    } else {
      return transformedRegex
          + MAKE_OPTIONAL_BLOCK.apply(DOT_CHARACTER_REGEX + FILLER_REGEX)
          + TERMINAL_REGEX_CHAR;
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
