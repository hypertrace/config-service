package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import java.util.Set;

public class RateLimitingConditionConverterUtils {
  private RateLimitingConditionConverterUtils() {}

  static final Set<KeyValueCondition.Type> CASE_INSENSITIVE_TYPES =
      Set.of(
          KeyValueCondition.Type.TYPE_USER_AGENT,
          KeyValueCondition.Type.TYPE_HOST,
          KeyValueCondition.Type.TYPE_URL,
          KeyValueCondition.Type.TYPE_HTTP_METHOD,
          KeyValueCondition.Type.TYPE_STATUS_CODE);

  static final Set<KeyValueCondition.Type> LIST_VALUE_MAP_TYPES =
      Set.of(
          KeyValueCondition.Type.TYPE_QUERY_PARAMETER,
          KeyValueCondition.Type.TYPE_REQUEST_BODY_PARAMETER);

  static final Set<KeyValueCondition.MatchOperator> INT_MATCH_OPERATORS =
      Set.of(
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_GREATER_THAN,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_LESS_THAN);

  static final Set<KeyValueCondition.MatchOperator> REGEX_MATCH_OPERATORS =
      Set.of(
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX);

  static final Set<KeyValueCondition.MatchOperator> ALL_MATCH_OPERATORS =
      Set.of(
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_GREATER_THAN,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_LESS_THAN);

  static String getPredicateJexlExpression(KeyValueCondition.MatchOperator operator, String value) {
    switch (operator) {
      case MATCH_OPERATOR_EQUALS:
        return String.format("predicate:equals('%s')", value);
      case MATCH_OPERATOR_NOT_EQUAL:
        return String.format("predicate:notEquals('%s')", value);
      case MATCH_OPERATOR_MATCHES_REGEX:
        return String.format("predicate:matchesRegex('%s')", value);
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return String.format("predicate:notMatchesRegex('%s')", value);
      case MATCH_OPERATOR_CONTAINS:
        return String.format("predicate:contains('%s')", value);
      case MATCH_OPERATOR_NOT_CONTAIN:
        return String.format("predicate:notContains('%s')", value);
      case MATCH_OPERATOR_GREATER_THAN:
        return String.format("predicate:greaterThan(%s)", value);
      case MATCH_OPERATOR_LESS_THAN:
        return String.format("predicate:lessThan(%s)", value);
      default:
        throw new IllegalArgumentException(
            "Invalid match operator in key value condition : " + operator);
    }
  }

  static String getJexlExpForType(KeyValueCondition.Type type) {
    switch (type) {
      case TYPE_URL:
        return "$s.getUrl()";
      case TYPE_HOST:
        return "$s.getHost()";
      case TYPE_HTTP_METHOD:
        return "$s.getMethod()";
      case TYPE_USER_AGENT:
        return "$s.getUserAgent()";
      case TYPE_REQUEST_BODY:
        return "$s.getRequestBody()";
      case TYPE_REQUEST_BODY_SIZE:
        return "$s.getRequestBody().length()";
      case TYPE_REQUEST_HEADERS_COUNT:
        return "$s.getRequestHeaders().size()";
      case TYPE_REQUEST_HEADER:
        return "$s.getRequestHeaders()";
      case TYPE_REQUEST_COOKIE:
        return "$s.getRequestCookies()";
      case TYPE_REQUEST_COOKIES_COUNT:
        return "$s.getRequestCookies().size()";
      case TYPE_QUERY_PARAMETER:
        return "$s.getQueryParams()";
      case TYPE_QUERY_PARAMS_COUNT:
        return "$s.getQueryParams().size()";
      case TYPE_REQUEST_BODY_PARAMETER:
        return "$s.getRequestBodyParams()";
      case TYPE_STATUS_CODE:
      case TYPE_RESPONSE_BODY:
      case TYPE_RESPONSE_HEADER:
      case TYPE_RESPONSE_COOKIE:
      case TYPE_RESPONSE_COOKIES_COUNT:
      case TYPE_RESPONSE_HEADERS_COUNT:
      case TYPE_RESPONSE_BODY_SIZE:
      case TYPE_RESPONSE_BODY_PARAMETER:
        // TODO : add exp after edge decision rules are supported on response
        throw new IllegalArgumentException(
            "Edge decision rule not applicable on response metadata type : " + type);
      default:
        throw new IllegalArgumentException("Invalid type in key value condition : " + type);
    }
  }

  static MatchOperator getMatchOperator(
      KeyValueCondition.Type type, KeyValueCondition.MatchOperator operator) {
    switch (operator) {
      case MATCH_OPERATOR_EQUALS:
        return CASE_INSENSITIVE_TYPES.contains(type)
            ? MatchOperator.MATCH_OPERATOR_EQ_IGNORE_CASE
            : MatchOperator.MATCH_OPERATOR_EQ;
      case MATCH_OPERATOR_NOT_EQUAL:
        return MatchOperator.MATCH_OPERATOR_NOT_EQ;
      case MATCH_OPERATOR_MATCHES_REGEX:
        return MatchOperator.MATCH_OPERATOR_LIKE;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return MatchOperator.MATCH_OPERATOR_NOT_LIKE;
      case MATCH_OPERATOR_CONTAINS:
      case MATCH_OPERATOR_NOT_CONTAIN:
        return MatchOperator.MATCH_OPERATOR_CONTAINS;
      case MATCH_OPERATOR_GREATER_THAN:
        return MatchOperator.MATCH_OPERATOR_GT;
      case MATCH_OPERATOR_LESS_THAN:
        return MatchOperator.MATCH_OPERATOR_LT;
      default:
        throw new IllegalArgumentException(
            "Invalid match operator in key value condition : " + operator);
    }
  }

  static String getJexlExpressionForKeyValueConditionType(KeyValueCondition.Type type) {
    switch (type) {
      case TYPE_REQUEST_HEADER:
        return "$s.getRequestHeaders()";
      case TYPE_REQUEST_BODY_PARAMETER:
        return "$s.getRequestBodyParams()";
      case TYPE_QUERY_PARAMETER:
        return "$s.getQueryParams()";
      case TYPE_REQUEST_COOKIE:
        return "$s.getRequestCookies()";
      case TYPE_RESPONSE_HEADER:
      case TYPE_RESPONSE_BODY_PARAMETER:
      case TYPE_RESPONSE_COOKIE:
        // TODO : add exp after edge decision rules are supported on response
        throw new IllegalArgumentException(
            "Edge decision rule not applicable on response metadata type : " + type);
      default:
        throw new IllegalArgumentException("Invalid type for key value set expression: " + type);
    }
  }
}
