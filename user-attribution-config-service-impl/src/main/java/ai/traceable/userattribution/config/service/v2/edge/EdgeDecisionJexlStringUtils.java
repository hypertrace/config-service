package ai.traceable.userattribution.config.service.v2.edge;

final class EdgeDecisionJexlStringUtils {
  private EdgeDecisionJexlStringUtils() {}

  static String escapeSingleQuotes(String input) {
    return input.replace("'", "\\'");
  }

  static String escapeDoubleQuotes(String input) {
    return input.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
