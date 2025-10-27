package ai.traceable.customsignature.config.service.rules.converter.expression;

import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import java.util.Set;

public class CustomSignatureExpressionConverterUtils {
  private static final Set<MatchKey> CASE_INSENSITIVE_TYPES =
      Set.of(
          MatchKey.MATCH_KEY_USER_AGENT,
          MatchKey.MATCH_KEY_HOST,
          MatchKey.MATCH_KEY_URL,
          MatchKey.MATCH_KEY_HTTP_METHOD,
          MatchKey.MATCH_KEY_STATUS_CODE);

  static String getPredicateJexlExpression(MatchOperator matchOperator, String matchValue) {
    switch (matchOperator) {
      case MATCH_OPERATOR_EQUALS:
        return String.format("predicate:equals('%s')", matchValue);
      case MATCH_OPERATOR_NOT_EQUAL:
        return String.format("predicate:notEquals('%s')", matchValue);
      case MATCH_OPERATOR_MATCHES_REGEX:
        return String.format("predicate:matchesRegex('%s')", matchValue);
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return String.format("predicate:notMatchesRegex('%s')", matchValue);
      case MATCH_OPERATOR_CONTAINS:
        return String.format("predicate:contains('%s')", matchValue);
      case MATCH_OPERATOR_NOT_CONTAIN:
        return String.format("predicate:notContains('%s')", matchValue);
      case MATCH_OPERATOR_GREATER_THAN:
        return String.format("predicate:greaterThan(%s)", matchValue);
      case MATCH_OPERATOR_LESS_THAN:
        return String.format("predicate:lessThan(%s)", matchValue);
      default:
        throw new IllegalArgumentException("Invalid match operator: " + matchOperator);
    }
  }

  static String getJexlExpressionForTag(KeyValueTag tag) {
    switch (tag) {
      case KEY_VALUE_TAG_HEADER:
        return "$s.getRequestHeaders()";
      case KEY_VALUE_TAG_BODY_PARAMETER:
        return "$s.getRequestBodyParams()";
      case KEY_VALUE_TAG_QUERY_PARAMETER:
        return "$s.getQueryParams()";
      case KEY_VALUE_TAG_COOKIE:
        return "$s.getRequestCookies()";
      default:
        throw new IllegalArgumentException("Invalid tag: " + tag);
    }
  }

  static String getJexlExpressionForMatchKey(MatchKey key) {
    switch (key) {
      case MATCH_KEY_URL:
        return "$s.getUrl()";
      case MATCH_KEY_HOST:
        return "$s.getHost()";
      case MATCH_KEY_HTTP_METHOD:
        return "$s.getMethod()";
      case MATCH_KEY_USER_AGENT:
        return "$s.getUserAgent()";
      case MATCH_KEY_HEADER_NAME:
      case MATCH_KEY_HEADER_VALUE:
        return "$s.getRequestHeaders()";
      case MATCH_KEY_BODY_PARAMETER_NAME:
      case MATCH_KEY_BODY_PARAMETER_VALUE:
        return "$s.getRequestBodyParams()";
      case MATCH_KEY_BODY:
        return "$s.getRequestBody()";
      case MATCH_KEY_QUERY_PARAMETER_NAME:
      case MATCH_KEY_QUERY_PARAMETER_VALUE:
        return "$s.getQueryParams()";
      case MATCH_KEY_COOKIE_NAME:
      case MATCH_KEY_COOKIE_VALUE:
        return "$s.getRequestCookies()";
      case MATCH_KEY_BODY_SIZE:
        return "$s.getRequestBody().length()";
      case MATCH_KEY_QUERY_PARAMS_COUNT:
        return "$s.getQueryParams().size()";
      case MATCH_KEY_HEADERS_COUNT:
        return "$s.getRequestHeaders().size()";
      case MATCH_KEY_COOKIES_COUNT:
        return "$s.getRequestCookies().size()";
      case MATCH_KEY_STATUS_CODE:
        // TODO : add exp after edge decision rules are supported on response
        throw new IllegalArgumentException(
            "Edge decision rule not applicable on response metadata type : " + key);
      default:
        throw new IllegalArgumentException("Invalid match key in key value expression : " + key);
    }
  }

  static String getJexlExpressionForLhsRhsMatchKey(MatchKey key) {
    switch (key) {
      case MATCH_KEY_HEADER_NAME:
        return "$s.getRequestHeaders()";
      case MATCH_KEY_BODY_PARAMETER_NAME:
        return "$s.getRequestBodyParams()";
      case MATCH_KEY_QUERY_PARAMETER_NAME:
        return "$s.getQueryParams()";
      case MATCH_KEY_COOKIE_NAME:
        return "$s.getRequestCookies()";
      case MATCH_KEY_PARAMETER_NAME:
        return "$s.getRequestParameters()";
      default:
        throw new IllegalArgumentException("Invalid match key in lhs rhs keys expression : " + key);
    }
  }

  static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
      getDataTransformationOperator(
          boolean isMatchKeyCaseInsensitive, MatchOperator matchOperator) {
    switch (matchOperator) {
      case MATCH_OPERATOR_EQUALS:
        return isMatchKeyCaseInsensitive
            ? ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
                .MATCH_OPERATOR_EQ_IGNORE_CASE
            : ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_EQ;
      case MATCH_OPERATOR_NOT_EQUAL:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
            .MATCH_OPERATOR_NOT_EQ;
      case MATCH_OPERATOR_MATCHES_REGEX:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
            .MATCH_OPERATOR_LIKE;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
            .MATCH_OPERATOR_NOT_LIKE;
      case MATCH_OPERATOR_CONTAINS:
      case MATCH_OPERATOR_NOT_CONTAIN:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
            .MATCH_OPERATOR_CONTAINS;
      case MATCH_OPERATOR_GREATER_THAN:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_GT;
      case MATCH_OPERATOR_LESS_THAN:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_LT;
      default:
        throw new IllegalArgumentException(
            "Invalid match operator in key value expression : " + matchOperator);
    }
  }

  static boolean isMatchKeyCaseInsensitive(MatchKey matchKey) {
    return CASE_INSENSITIVE_TYPES.contains(matchKey);
  }
}
