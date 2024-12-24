package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_INT;
import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.ratelimiting.service.v2.rules.ValidatorUtils.KEY_NULL_CONDITION_TYPES;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.edge.decision.config.service.v1.BinaryOperator;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.v1.MatchOperator;
import ai.traceable.edge.decision.config.service.v1.StructuredMatchCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
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
  private static final Set<KeyValueCondition.MatchOperator> INT_MATCH_OPERATORS =
      Set.of(
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_GREATER_THAN,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_LESS_THAN);
  private static final Set<KeyValueCondition.MatchOperator> REGEX_MATCH_OPERATORS =
      Set.of(
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX,
          KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX);

  @Override
  public MatchCondition buildMatchCondition(
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
                      .setName("lhs")
                      .setType(fieldType)
                      .addRules(
                          DerivationRule.newBuilder()
                              .setTransformationConfig(
                                  DataTransformationConfig.newBuilder()
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
      return matchConditionBuilder.build();
    }
    throw new IllegalArgumentException("Invalid key value condition : " + keyValueCondition);
  }

  private String getJexlExp(KeyValueCondition.Type type) {
    switch (type) {
      case TYPE_URL:
        return "$s.getUrl()";
      case TYPE_HOST:
        return "$s.getHost()";
      case TYPE_HTTP_METHOD:
        return "$s.getHttpMethod()";
      case TYPE_USER_AGENT:
        return "$s.getUserAgent()";
      case TYPE_REQUEST_BODY:
        return "$s.getRequestBody()";
      case TYPE_REQUEST_BODY_SIZE:
        return "$s.getRequestBody().length()";
      case TYPE_REQUEST_HEADERS_COUNT:
        return "$s.getRequestHeaders().size()";
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
