package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_INT;
import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.edge.decision.converter.utils.Constants.ATTRIBUTE_NAME_LHS;
import static ai.traceable.ratelimiting.service.v2.rules.ValidatorUtils.KEY_NULL_CONDITION_TYPES;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.GenericMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import java.util.Collections;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingKeyValueConditionConverter implements RateLimitingConditionConverter {

  private static final Set<KeyValueCondition.Type> CASE_INSENSITIVE_TYPES =
      Set.of(
          KeyValueCondition.Type.TYPE_USER_AGENT,
          KeyValueCondition.Type.TYPE_HOST,
          KeyValueCondition.Type.TYPE_URL,
          KeyValueCondition.Type.TYPE_HTTP_METHOD,
          KeyValueCondition.Type.TYPE_STATUS_CODE);
  private static final Set<KeyValueCondition.Type> LIST_VALUE_MAP_TYPES =
      Set.of(
          KeyValueCondition.Type.TYPE_QUERY_PARAMETER,
          KeyValueCondition.Type.TYPE_REQUEST_BODY_PARAMETER);
  private static final Set<KeyValueCondition.MatchOperator> INT_MATCH_OPERATORS =
      Set.of(
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_GREATER_THAN,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_LESS_THAN);
  private static final Set<KeyValueCondition.MatchOperator> REGEX_MATCH_OPERATORS =
      Set.of(
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX);
  private static final Set<KeyValueCondition.MatchOperator> ALL_MATCH_OPERATORS =
      Set.of(
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_EQUAL,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_GREATER_THAN,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_LESS_THAN);

  @Override
  public MatchConditionDetails buildMatchCondition(
      RequestContext requestContext, LeafCondition leafCondition) {
    KeyValueCondition keyValueCondition = leafCondition.getKeyValueCondition();
    KeyValueCondition.Type type = keyValueCondition.getType();
    if (KEY_NULL_CONDITION_TYPES.contains(type)) {
      KeyValueCondition.StringCondition stringCondition = keyValueCondition.getValueCondition();
      BinaryOperator.Builder builder =
          BinaryOperator.newBuilder()
              .setMatchOperator(getOp(type, keyValueCondition.getValueCondition().getOperator()));
      FieldType fieldType = FIELD_TYPE_STR;
      if (INT_MATCH_OPERATORS.contains(stringCondition.getOperator())) {
        builder.setNumberValue(Double.parseDouble(stringCondition.getValue()));
        fieldType = FIELD_TYPE_INT;
      } else if (REGEX_MATCH_OPERATORS.contains(stringCondition.getOperator())) {
        builder.setRegex(stringCondition.getValue());
      } else {
        builder.setStringValue(stringCondition.getValue());
      }
      StructuredMatchCondition structuredMatchCondition =
          StructuredMatchCondition.newBuilder()
              .setLhs(
                  AttributeDerivationMapping.newBuilder()
                      .setName(ATTRIBUTE_NAME_LHS)
                      .setType(fieldType)
                      .addRules(
                          DerivationRule.newBuilder()
                              .setTransformationConfig(
                                  DataTransformationConfig.newBuilder()
                                      .setOutputType(fieldType)
                                      .setJexlExpression(
                                          JexlExpressionConfig.newBuilder()
                                              .setJexlExpression(getJexlExp(type))))))
              .setBinaryOperator(builder)
              .build();
      MatchCondition.Builder matchConditionBuilder =
          MatchCondition.newBuilder().setStructuredMatchCondition(structuredMatchCondition);
      // no first class support of not contains currently
      if (stringCondition
          .getOperator()
          .equals(KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN)) {
        matchConditionBuilder.setNegate(true);
      }
      return new MatchConditionDetails(
          matchConditionBuilder.build(), Collections.emptyList(), Collections.emptyList());
    } else {
      // types supporting both key and value condition are stored as Map<String, String> or
      // Map<String, List<String>> in the edge-decision-service
      String jexlExp;
      if (keyValueCondition.hasKeyCondition() && keyValueCondition.hasValueCondition()) {
        jexlExp =
            LIST_VALUE_MAP_TYPES.contains(keyValueCondition.getType())
                ? String.format(
                    "map:match(%s, %s, %s, %s)",
                    getJexlExp(keyValueCondition.getType()),
                    getPredicateJexlExp(keyValueCondition.getKeyCondition()),
                    getPredicateJexlExp(keyValueCondition.getValueCondition()),
                    ALL_MATCH_OPERATORS.contains(
                        keyValueCondition.getValueCondition().getOperator()))
                : String.format(
                    "map:match(%s, %s, %s)",
                    getJexlExp(keyValueCondition.getType()),
                    getPredicateJexlExp(keyValueCondition.getKeyCondition()),
                    getPredicateJexlExp(keyValueCondition.getValueCondition()));
      } else {
        jexlExp =
            String.format(
                "map:match(%s, %s, %s)",
                getJexlExp(keyValueCondition.getType()),
                getPredicateJexlExp(keyValueCondition.getKeyCondition()),
                ALL_MATCH_OPERATORS.contains(keyValueCondition.getKeyCondition().getOperator()));
      }
      return new MatchConditionDetails(
          MatchCondition.newBuilder()
              .setGenericMatchCondition(
                  GenericMatchCondition.newBuilder()
                      .setJexlExpression(
                          JexlExpressionConfig.newBuilder().setJexlExpression(jexlExp)))
              .build(),
          Collections.emptyList(),
          Collections.emptyList());
    }
  }

  private String getPredicateJexlExp(KeyValueCondition.StringCondition stringCondition) {
    switch (stringCondition.getOperator()) {
      case MATCH_OPERATOR_EQUALS:
        return String.format("predicate:equals('%s')", stringCondition.getValue());
      case MATCH_OPERATOR_NOT_EQUAL:
        return String.format("predicate:notEquals('%s')", stringCondition.getValue());
      case MATCH_OPERATOR_MATCHES_REGEX:
        return String.format("predicate:matchesRegex('%s')", stringCondition.getValue());
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        return String.format("predicate:notMatchesRegex('%s')", stringCondition.getValue());
      case MATCH_OPERATOR_CONTAINS:
        return String.format("predicate:contains('%s')", stringCondition.getValue());
      case MATCH_OPERATOR_NOT_CONTAIN:
        return String.format("predicate:notContains('%s')", stringCondition.getValue());
      case MATCH_OPERATOR_GREATER_THAN:
        return String.format("predicate:greaterThan(%s)", stringCondition.getValue());
      case MATCH_OPERATOR_LESS_THAN:
        return String.format("predicate:lessThan(%s)", stringCondition.getValue());
      default:
        throw new IllegalArgumentException(
            "Invalid match operator in key value condition : " + stringCondition.getOperator());
    }
  }

  private String getJexlExp(KeyValueCondition.Type type) {
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

  private MatchOperator getOp(
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

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.KEY_VALUE_CONDITION;
  }
}
