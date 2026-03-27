package ai.traceable.fraud.policy.config.service.converter;

import com.google.protobuf.Value;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Shared utility methods for building JEXL expression strings. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JexlExpressionUtils {

  public static final String SPAN_VAR = "$s";

  public static String escapeJexlString(String value) {
    return value.replace("'", "\\'");
  }

  /**
   * Converts a protobuf Value to its raw string representation (no JEXL quoting). Suitable for use
   * as RHS in numeric/boolean comparisons or as input to further formatting.
   */
  public static String valueToString(Value value) {
    switch (value.getKindCase()) {
      case STRING_VALUE:
        return value.getStringValue();
      case NUMBER_VALUE:
        double num = value.getNumberValue();
        if (num == Math.floor(num) && !Double.isInfinite(num)) {
          return String.valueOf((long) num);
        }
        return String.valueOf(num);
      case BOOL_VALUE:
        return String.valueOf(value.getBoolValue());
      case NULL_VALUE:
        return "null";
      default:
        return value.toString();
    }
  }

  /**
   * Builds an OR-joined equality expression for a getter and a list of values. e.g. {@code
   * ($s.getEnvironment().equals('prod') || $s.getEnvironment().equals('staging'))}
   */
  public static String toEqualsExpr(String getter, List<String> values) {
    if (values == null || values.isEmpty()) {
      return "";
    }
    String expr =
        values.stream()
            .map(v -> getter + ".equals('" + escapeJexlString(v) + "')")
            .collect(Collectors.joining(" || "));
    return values.size() > 1 ? "(" + expr + ")" : expr;
  }

  /**
   * Builds an OR-joined regex match expression for URL patterns. e.g. {@code ($s.getUrl() =~
   * '/api/.*' || $s.getUrl() =~ '/v2/.*')}
   */
  public static String toUrlRegexExpr(Set<String> urlRegexes) {
    String expr =
        urlRegexes.stream()
            .map(pattern -> SPAN_VAR + ".getUrl() =~ '" + escapeJexlString(pattern) + "'")
            .collect(Collectors.joining(" || "));
    return urlRegexes.size() > 1 ? "(" + expr + ")" : expr;
  }

  /**
   * Converts a JSON path key (e.g. "$.inputParameters.Request.SecurityHint") into chained bracket
   * notation (e.g. "['inputParameters']['Request']['SecurityHint']").
   */
  public static String jsonPathToChainedBrackets(String jsonPath) {
    String path = jsonPath;
    if (path.startsWith("$.")) {
      path = path.substring(2);
    } else if (path.startsWith("$")) {
      path = path.substring(1);
    } else if (path.startsWith(".")) {
      path = path.substring(1);
    }
    if (path.isEmpty()) {
      return "";
    }
    String[] segments = path.split("\\.");
    StringBuilder sb = new StringBuilder();
    for (String segment : segments) {
      if (!segment.isEmpty()) {
        sb.append("['").append(escapeJexlString(segment)).append("']");
      }
    }
    return sb.toString();
  }

  /** Converts underscore-delimited text to PascalCase. e.g. "ip_address" → "IpAddress" */
  public static String toPascalCase(String input) {
    StringBuilder sb = new StringBuilder();
    boolean capitalizeNext = true;
    for (char c : input.toCharArray()) {
      if (c == '_') {
        capitalizeNext = true;
      } else if (capitalizeNext) {
        sb.append(Character.toUpperCase(c));
        capitalizeNext = false;
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /** Converts arbitrary text to snake_case. e.g. "Auth Token" → "auth_token" */
  public static String toSnakeCase(String input) {
    return input.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_").replaceAll("(^_)|(_$)", "");
  }
}
