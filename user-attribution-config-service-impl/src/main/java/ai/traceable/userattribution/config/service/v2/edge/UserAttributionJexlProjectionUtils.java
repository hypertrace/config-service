package ai.traceable.userattribution.config.service.v2.edge;

import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import java.util.List;

final class UserAttributionJexlProjectionUtils {
  private UserAttributionJexlProjectionUtils() {}

  static String applyAttributeProjection(String base, AttributeProjection projection) {
    String attrExpr = attributeToJexl(base, projection.getAttribute());
    return applyValueProjections(attrExpr, projection.getValueProjectionsList());
  }

  private static String attributeToJexl(String base, Attribute attribute) {
    switch (attribute.getAttributeCase()) {
      case REQUEST_HEADER:
        return keyMatchToJexl(base + ".getRequestHeaders()", attribute.getRequestHeader());
      case REQUEST_COOKIE:
        return keyMatchToJexl(base + ".getRequestCookies()", attribute.getRequestCookie());
      case REQUEST_QUERY_PARAMETER:
        return keyMatchToJexl(base + ".getQueryParams()", attribute.getRequestQueryParameter());
      case RESPONSE_HEADER:
        return keyMatchToJexl(base + ".getResponseHeaders()", attribute.getResponseHeader());
      case RESPONSE_COOKIE:
        return keyMatchToJexl(base + ".getResponseCookies()", attribute.getResponseCookie());
      case SPAN_ATTRIBUTE:
        return keyMatchToJexl(base + ".getSpanAttributes()", attribute.getSpanAttribute());
      case REQUEST_BODY:
        return base + ".getRequestBody()";
      case RESPONSE_BODY:
        return base + ".getResponseBody()";
      default:
        throw new IllegalArgumentException(
            "Unsupported attribute: " + attribute.getAttributeCase());
    }
  }

  private static String keyMatchToJexl(String baseMapExpr, KeyMatch keyMatch) {
    String key = escapeSingleQuotes(keyMatch.getMatchKey());
    switch (keyMatch.getOperator()) {
      case KEY_MATCH_OPERATOR_EQUALS:
        return String.format("%s.get('%s')", baseMapExpr, key);
      case KEY_MATCH_OPERATOR_STARTS_WITH:
        return String.format("map:matchingValue(%s, predicate:startsWith('%s'))", baseMapExpr, key);
      case KEY_MATCH_OPERATOR_CONTAINS:
        return String.format("map:matchingValue(%s, predicate:contains('%s'))", baseMapExpr, key);
      case KEY_MATCH_OPERATOR_MATCHES_REGEX:
        return String.format(
            "map:matchingValue(%s, predicate:matchesRegex('%s'))", baseMapExpr, key);
      default:
        throw new IllegalArgumentException(
            "Unsupported key match operator: " + keyMatch.getOperator());
    }
  }

  static String applyValueProjections(String inputJexl, List<ValueProjection> projections) {
    String current = inputJexl;
    for (ValueProjection projection : projections) {
      switch (projection.getProjectionCase()) {
        case REGEX_CAPTURE_GROUP:
          current =
              current
                  + ".replaceAll(\""
                  + escapeDoubleQuotes(projection.getRegexCaptureGroup().getRegex())
                  + "\", \"$1\")";
          break;
        case JSON_PATH:
          current =
              "traceableTransformUtils:extractJsonPathValue("
                  + current
                  + ", \""
                  + escapeDoubleQuotes(projection.getJsonPath().getPath())
                  + "\")";
          break;
        case JWT_PAYLOAD_CLAIM:
          current =
              String.format(
                  "traceableTransformUtils:extractJWTPath(%s, \"%s\", \"PAYLOAD\")",
                  current, escapeDoubleQuotes(projection.getJwtPayloadClaim().getClaimKey()));
          break;
        case BASE64:
          current = "traceableTransformUtils:base64Decode(" + current + ")";
          break;
        case URL_DECODE_STRING:
          current =
              "traceableTransformUtils:urlDecode("
                  + current
                  + ", "
                  + projection.getUrlDecodeString().getQuotePlus()
                  + ")";
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported value projection: " + projection.getProjectionCase());
      }
    }
    return current;
  }

  private static String escapeSingleQuotes(String input) {
    return EdgeDecisionJexlStringUtils.escapeSingleQuotes(input);
  }

  private static String escapeDoubleQuotes(String input) {
    return EdgeDecisionJexlStringUtils.escapeDoubleQuotes(input);
  }
}
