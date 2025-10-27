package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_HOST;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_HTTP_METHOD;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_BODY;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_BODY_PARAMETER;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_BODY_SIZE;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_COOKIE;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_COOKIES_COUNT;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_HEADER;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_RESPONSE_HEADERS_COUNT;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_STATUS_CODE;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_URL;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_USER_AGENT;

import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadata;
import java.util.Set;

public class DetectionExclusionRuleConditionConverterUtils {
  static final Set<KeyMetadata> CASE_INSENSITIVE_KEY_METADATA_TYPES =
      Set.of(
          KEY_METADATA_USER_AGENT,
          KEY_METADATA_HOST,
          KEY_METADATA_URL,
          KEY_METADATA_HTTP_METHOD,
          KEY_METADATA_STATUS_CODE);

  static final Set<KeyMetadata> RESPONSE_KEY_METADATA =
      Set.of(
          KEY_METADATA_RESPONSE_BODY,
          KEY_METADATA_RESPONSE_HEADER,
          KEY_METADATA_RESPONSE_COOKIE,
          KEY_METADATA_RESPONSE_BODY_PARAMETER,
          KEY_METADATA_RESPONSE_BODY_SIZE,
          KEY_METADATA_RESPONSE_HEADERS_COUNT,
          KEY_METADATA_RESPONSE_COOKIES_COUNT);

  static String getPredicateJexlExpression(
      ai.traceable.detection.exclusion.config.service.v1.MatchCondition matchCondition) {
    switch (matchCondition.getOperator()) {
      case MATCH_OPERATOR_EQUALS:
        return String.format("predicate:equals('%s')", matchCondition.getValue().getStringValue());
      case MATCH_OPERATOR_NOT_EQUAL:
        return String.format(
            "predicate:notEquals('%s')", matchCondition.getValue().getStringValue());
      case MATCH_OPERATOR_MATCHES_REGEX:
        return String.format(
            "predicate:matchesRegex('%s')", matchCondition.getValue().getStringValue());
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return String.format(
            "predicate:notMatchesRegex('%s')", matchCondition.getValue().getStringValue());
      case MATCH_OPERATOR_GREATER_THAN:
        return String.format(
            "predicate:greaterThan(%s)", matchCondition.getValue().getStringValue());
      case MATCH_OPERATOR_LESS_THAN:
        return String.format("predicate:lessThan(%s)", matchCondition.getValue().getStringValue());
      default:
        throw new IllegalArgumentException(
            "Invalid match operator in key value condition : " + matchCondition.getOperator());
    }
  }

  static String getJexlExpressionForAttributeMatchConditionKeyMetadata(KeyMetadata keyMetadata) {
    switch (keyMetadata) {
      case KEY_METADATA_URL:
        return "$s.getUrl()";
      case KEY_METADATA_HOST:
        return "$s.getHost()";
      case KEY_METADATA_HTTP_METHOD:
        return "$s.getMethod()";
      case KEY_METADATA_USER_AGENT:
        return "$s.getUserAgent()";
      case KEY_METADATA_REQUEST_BODY:
        return "$s.getRequestBody()";
      case KEY_METADATA_REQUEST_BODY_SIZE:
        return "$s.getRequestBody().length()";
      case KEY_METADATA_REQUEST_HEADERS_COUNT:
        return "$s.getRequestHeaders().size()";
      case KEY_METADATA_REQUEST_HEADER:
        return "$s.getRequestHeaders()";
      case KEY_METADATA_REQUEST_COOKIE:
        return "$s.getRequestCookies()";
      case KEY_METADATA_REQUEST_COOKIES_COUNT:
        return "$s.getRequestCookies().size()";
      case KEY_METADATA_QUERY_PARAMETER:
        return "$s.getQueryParams()";
      case KEY_METADATA_QUERY_PARAMS_COUNT:
        return "$s.getQueryParams().size()";
      case KEY_METADATA_REQUEST_BODY_PARAMETER:
        return "$s.getRequestBodyParams()";
      case KEY_METADATA_STATUS_CODE:
      case KEY_METADATA_RESPONSE_BODY:
      case KEY_METADATA_RESPONSE_HEADER:
      case KEY_METADATA_RESPONSE_COOKIE:
      case KEY_METADATA_RESPONSE_COOKIES_COUNT:
      case KEY_METADATA_RESPONSE_HEADERS_COUNT:
      case KEY_METADATA_RESPONSE_BODY_SIZE:
      case KEY_METADATA_RESPONSE_BODY_PARAMETER:
        // TODO : add exp after edge decision rules are supported on response
        throw new IllegalArgumentException(
            "Edge decision rule not applicable on response metadata type : " + keyMetadata);
      default:
        throw new IllegalArgumentException(
            "Invalid KeyMetadata type in key value condition : " + keyMetadata);
    }
  }

  static String getJexlExpressionForLhsRhsMatchConditionKeyMetadata(KeyMetadata keyMetadata) {
    switch (keyMetadata) {
      case KEY_METADATA_REQUEST_HEADER:
        return "$s.getRequestHeaders()";
      case KEY_METADATA_REQUEST_COOKIE:
        return "$s.getRequestCookies()";
      case KEY_METADATA_QUERY_PARAMETER:
        return "$s.getQueryParams()";
      case KEY_METADATA_REQUEST_BODY_PARAMETER:
        return "$s.getRequestBodyParams()";
      default:
        throw new IllegalArgumentException(
            "Invalid KeyMetadata type in lhs rhs keys match condition: " + keyMetadata);
    }
  }

  static MatchOperator getMatchOperator(
      boolean isKeyMetadataCaseInsensitive,
      ai.traceable.detection.exclusion.config.service.v1.MatchOperator operator) {
    switch (operator) {
      case MATCH_OPERATOR_EQUALS:
        return isKeyMetadataCaseInsensitive
            ? MatchOperator.MATCH_OPERATOR_EQ_IGNORE_CASE
            : MatchOperator.MATCH_OPERATOR_EQ;
      case MATCH_OPERATOR_NOT_EQUAL:
        return MatchOperator.MATCH_OPERATOR_NOT_EQ;
      case MATCH_OPERATOR_MATCHES_REGEX:
        return MatchOperator.MATCH_OPERATOR_LIKE;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return MatchOperator.MATCH_OPERATOR_NOT_LIKE;
      case MATCH_OPERATOR_GREATER_THAN:
        return MatchOperator.MATCH_OPERATOR_GT;
      case MATCH_OPERATOR_LESS_THAN:
        return MatchOperator.MATCH_OPERATOR_LT;
      default:
        throw new IllegalArgumentException(
            "Invalid match operator in key value condition : " + operator);
    }
  }

  static boolean isKeyMetadataCaseInsensitive(KeyMetadata keyMetadata) {
    return CASE_INSENSITIVE_KEY_METADATA_TYPES.contains(keyMetadata);
  }
}
