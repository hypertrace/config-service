package ai.traceable.config.utils;

public class RegexUtils {
  public static String escapeRegex(String value) {
    return value.replaceAll("\\W", "\\\\$0");
  }
}
