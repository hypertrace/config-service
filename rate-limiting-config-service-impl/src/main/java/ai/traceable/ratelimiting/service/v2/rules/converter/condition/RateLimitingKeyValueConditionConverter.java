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
    if (keyValueCondition.hasLhsRhsCondition()) {
      return buildLhsRhsMatchCondition(keyValueCondition.getLhsRhsCondition());
    }
    return keyValueCondition.hasStaticValueCondition()
        ? buildMatchCondition(keyValueCondition)
        : buildDeprecatedMatchCondition(keyValueCondition);
  }

  MatchConditionDetails buildLhsRhsMatchCondition(
      KeyValueCondition.LhsRhsKeysCondition lhsRhsKeysCondition) {
    // Access key conditions
    KeyValueCondition.KeyCondition lhsKeyCondition = lhsRhsKeysCondition.getLhsKeyCondition();
    KeyValueCondition.KeyCondition rhsKeyCondition = lhsRhsKeysCondition.getRhsKeyCondition();
    KeyValueCondition.MatchOperator matchOperator = lhsRhsKeysCondition.getLhsRhsMatchOperator();

    // Extract key types
    KeyValueCondition.Type lhsKeyType = lhsKeyCondition.getKeyType();
    KeyValueCondition.Type rhsKeyType = rhsKeyCondition.getKeyType();

    // Extract match operator conditions
    KeyValueCondition.MatchOperatorCondition lhsMatchOperatorCondition =
        lhsKeyCondition.getKeyMatchOperatorCondition();
    KeyValueCondition.MatchOperatorCondition rhsMatchOperatorCondition =
        rhsKeyCondition.getKeyMatchOperatorCondition();

    // Extract operators and values
    KeyValueCondition.MatchOperator lhsOperator = lhsMatchOperatorCondition.getOperator();
    KeyValueCondition.MatchOperator rhsOperator = rhsMatchOperatorCondition.getOperator();

    // Extract values - just get the string value directly
    String lhsValue = "";
    if (lhsMatchOperatorCondition.hasValue()) {
      lhsValue = lhsMatchOperatorCondition.getValue().getStringValue();
    }

    String rhsValue = "";
    if (rhsMatchOperatorCondition.hasValue()) {
      rhsValue = rhsMatchOperatorCondition.getValue().getStringValue();
    }

    // Get the JEXL expressions for LHS and RHS key sets
    String lhsJexlExp = getJexlExpForKeyValueConditionType(lhsKeyType);
    String rhsJexlExp = getJexlExpForKeyValueConditionType(rhsKeyType);

    // Build the predicates for LHS and RHS values
    String lhsPredicate = getPredicateJexlExp(lhsOperator, lhsValue);
    String rhsPredicate = getPredicateJexlExp(rhsOperator, rhsValue);

    // Construct the full JEXL expression using map:match
    String jexlExp =
        String.format(
            "map:match(%s, %s, %s, %s, %s)",
            lhsJexlExp,
            rhsJexlExp,
            lhsPredicate,
            rhsPredicate,
            matchOperator.name()); // Use the match operator from the condition

    // Build and return the match condition details
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

  MatchConditionDetails buildMatchCondition(KeyValueCondition keyValueCondition) {
    KeyValueCondition.StaticValueCondition staticValueCondition =
        keyValueCondition.getStaticValueCondition();
    KeyValueCondition.Type type = staticValueCondition.getKeyCondition().getKeyType();
    boolean hasKeyCondition = staticValueCondition.getKeyCondition().hasKeyMatchOperatorCondition();
    boolean hasValueCondition = staticValueCondition.hasValueMatchOperatorCondition();
    KeyValueCondition.MatchOperator keyOperator =
        hasKeyCondition
            ? staticValueCondition.getKeyCondition().getKeyMatchOperatorCondition().getOperator()
            : null;
    KeyValueCondition.MatchOperator valueOperator =
        hasValueCondition
            ? staticValueCondition.getValueMatchOperatorCondition().getOperator()
            : null;
    String value =
        hasValueCondition
            ? staticValueCondition.getValueMatchOperatorCondition().getValue().getStringValue()
            : null;
    String key =
        hasKeyCondition
            ? staticValueCondition
                .getKeyCondition()
                .getKeyMatchOperatorCondition()
                .getValue()
                .getStringValue()
            : null;
    return buildConditionalMatchOperatorCondition(
        type, hasKeyCondition, hasValueCondition, keyOperator, valueOperator, key, value);
  }

  @Deprecated
  MatchConditionDetails buildDeprecatedMatchCondition(KeyValueCondition keyValueCondition) {
    KeyValueCondition.Type type = keyValueCondition.getType();
    boolean hasKeyCondition = keyValueCondition.hasKeyCondition();
    boolean hasValueCondition = keyValueCondition.hasValueCondition();
    KeyValueCondition.MatchOperator keyOperator =
        hasKeyCondition ? keyValueCondition.getKeyCondition().getOperator() : null;
    KeyValueCondition.MatchOperator valueOperator =
        hasValueCondition ? keyValueCondition.getValueCondition().getOperator() : null;
    String value = hasValueCondition ? keyValueCondition.getValueCondition().getValue() : null;
    String key = hasKeyCondition ? keyValueCondition.getKeyCondition().getValue() : null;
    return buildConditionalMatchOperatorCondition(
        type, hasKeyCondition, hasValueCondition, keyOperator, valueOperator, key, value);
  }

  MatchConditionDetails buildConditionalMatchOperatorCondition(
      KeyValueCondition.Type type,
      boolean hasKeyCondition,
      boolean hasValueCondition,
      KeyValueCondition.MatchOperator keyOperator,
      KeyValueCondition.MatchOperator valueOperator,
      String key,
      String value) {
    if (hasValueCondition && KEY_NULL_CONDITION_TYPES.contains(type)) {
      BinaryOperator.Builder builder =
          BinaryOperator.newBuilder().setMatchOperator(getOp(type, valueOperator));
      FieldType fieldType = FIELD_TYPE_STR;
      if (INT_MATCH_OPERATORS.contains(valueOperator)) {
        builder.setNumberValue(Double.parseDouble(value));
        fieldType = FIELD_TYPE_INT;
      } else if (REGEX_MATCH_OPERATORS.contains(valueOperator)) {
        builder.setRegex(value);
      } else {
        builder.setStringValue(value);
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
      if (valueOperator.equals(KeyValueCondition.MatchOperator.MATCH_OPERATOR_NOT_CONTAIN)) {
        matchConditionBuilder.setNegate(true);
      }
      return new MatchConditionDetails(
          matchConditionBuilder.build(), Collections.emptyList(), Collections.emptyList());
    } else {
      // types supporting both key and value condition are stored as Map<String, String> or
      // Map<String, List<String>> in the edge-decision-service
      String jexlExp = "";
      if (hasKeyCondition && hasValueCondition) {
        jexlExp =
            LIST_VALUE_MAP_TYPES.contains(type)
                ? String.format(
                    "map:match(%s, %s, %s, %s)",
                    getJexlExp(type),
                    getPredicateJexlExp(keyOperator, key),
                    getPredicateJexlExp(valueOperator, value),
                    ALL_MATCH_OPERATORS.contains(valueOperator))
                : String.format(
                    "map:match(%s, %s, %s)",
                    getJexlExp(type),
                    getPredicateJexlExp(keyOperator, key),
                    getPredicateJexlExp(valueOperator, value));
      } else if (hasKeyCondition) {
        jexlExp =
            String.format(
                "map:match(%s, %s, %s)",
                getJexlExp(type),
                getPredicateJexlExp(keyOperator, key),
                ALL_MATCH_OPERATORS.contains(keyOperator));
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

  private String getPredicateJexlExp(KeyValueCondition.MatchOperator operator, String value) {
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

  private String getJexlExpForKeyValueConditionType(KeyValueCondition.Type type) {
    switch (type) {
      case TYPE_REQUEST_HEADER:
        return "$s.getRequestHeaders().getKeySet()";
      case TYPE_RESPONSE_HEADER:
        return "$s.getResponseHeaders().getKeySet()";
      case TYPE_REQUEST_BODY_PARAMETER:
        return "$s.getRequestBodyParams().getKeySet()";
      case TYPE_RESPONSE_BODY_PARAMETER:
        return "$s.getResponseBodyParams().getKeySet()";
      case TYPE_QUERY_PARAMETER:
        return "$s.getQueryParams().getKeySet()";
      case TYPE_REQUEST_COOKIE:
        return "$s.getRequestCookies().getKeySet()";
      case TYPE_RESPONSE_COOKIE:
        return "$s.getResponseCookies().getKeySet()";
      default:
        throw new IllegalArgumentException("Invalid type for key set expression: " + type);
    }
  }

  @Override
  public LeafCondition.ConditionCase getConditionCase() {
    return LeafCondition.ConditionCase.KEY_VALUE_CONDITION;
  }
}
