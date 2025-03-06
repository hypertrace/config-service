package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_HOST;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_HTTP_METHOD;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_QUERY_PARAMETER;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_BODY_PARAMETER;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_STATUS_CODE;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_URL;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_USER_AGENT;
import static ai.traceable.edge.decision.converter.utils.Constants.ATTRIBUTE_NAME_LHS;

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
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadata;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionConditionValidator;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DetectionExclusionRuleAttributeMatchConditionConverter
    implements DetectionExclusionRuleConditionConverter {

  private static final Set<KeyMetadata> CASE_INSENSITIVE_KEY_METADATA_TYPES =
      Set.of(
          KEY_METADATA_USER_AGENT,
          KEY_METADATA_HOST,
          KEY_METADATA_URL,
          KEY_METADATA_HTTP_METHOD,
          KEY_METADATA_STATUS_CODE);
  private static final Set<KeyMetadata> LIST_VALUE_MAP_KEY_METADATA_TYPES =
      Set.of(KEY_METADATA_QUERY_PARAMETER, KEY_METADATA_REQUEST_BODY_PARAMETER);
  private static final Set<ai.traceable.detection.exclusion.config.service.v1.MatchOperator>
      INT_MATCH_OPERATORS =
          Set.of(
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_GREATER_THAN,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_LESS_THAN);
  private static final Set<ai.traceable.detection.exclusion.config.service.v1.MatchOperator>
      REGEX_MATCH_OPERATORS =
          Set.of(
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_MATCHES_REGEX,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_NOT_MATCH_REGEX);
  private static final Set<ai.traceable.detection.exclusion.config.service.v1.MatchOperator>
      ALL_MATCH_OPERATORS =
          Set.of(
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_NOT_EQUAL,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_NOT_MATCH_REGEX,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_GREATER_THAN,
              ai.traceable.detection.exclusion.config.service.v1.MatchOperator
                  .MATCH_OPERATOR_LESS_THAN);

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, DetectionExclusionCondition condition) {
    SpanAttributeMatchCondition attributeMatchCondition = condition.getAttributeMatchCondition();
    KeyMetadata keyMetadata = attributeMatchCondition.getKeyMatchCondition().getMetadata();
    if (DetectionExclusionConditionValidator.KEY_NULL_META_DATAS.contains(keyMetadata)) {
      ai.traceable.detection.exclusion.config.service.v1.MatchCondition matchCondition =
          attributeMatchCondition.getValueMatchCondition();
      BinaryOperator.Builder builder =
          BinaryOperator.newBuilder()
              .setMatchOperator(getOp(keyMetadata, matchCondition.getOperator()));
      FieldType fieldType = FieldType.FIELD_TYPE_STR;
      if (INT_MATCH_OPERATORS.contains(matchCondition.getOperator())) {
        builder.setNumberValue(Double.parseDouble(matchCondition.getValue().getStringValue()));
        fieldType = FieldType.FIELD_TYPE_INT;
      } else if (REGEX_MATCH_OPERATORS.contains(matchCondition.getOperator())) {
        builder.setRegex(matchCondition.getValue().getStringValue());
      } else {
        builder.setStringValue(matchCondition.getValue().getStringValue());
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
                                      .setJexlExpression(
                                          JexlExpressionConfig.newBuilder()
                                              .setJexlExpression(getJexlExp(keyMetadata))))))
              .setBinaryOperator(builder)
              .build();
      return MatchCondition.newBuilder()
          .setStructuredMatchCondition(structuredMatchCondition)
          .build();
    } else {
      // types supporting both key and value condition are stored as Map<String, String> or
      // Map<String, List<String>> in the edge-decision-service
      String jexlExp;
      if (attributeMatchCondition.getKeyMatchCondition().hasMatchCondition()
          && attributeMatchCondition.hasValueMatchCondition()) {
        jexlExp =
            LIST_VALUE_MAP_KEY_METADATA_TYPES.contains(keyMetadata)
                ? String.format(
                    "map:match(%s, %s, %s, %s)",
                    getJexlExp(keyMetadata),
                    getPredicateJexlExp(
                        attributeMatchCondition.getKeyMatchCondition().getMatchCondition()),
                    getPredicateJexlExp(attributeMatchCondition.getValueMatchCondition()),
                    ALL_MATCH_OPERATORS.contains(
                        attributeMatchCondition.getValueMatchCondition().getOperator()))
                : String.format(
                    "map:match(%s, %s, %s)",
                    getJexlExp(keyMetadata),
                    getPredicateJexlExp(
                        attributeMatchCondition.getKeyMatchCondition().getMatchCondition()),
                    getPredicateJexlExp(attributeMatchCondition.getValueMatchCondition()));
      } else {
        jexlExp =
            String.format(
                "map:match(%s, %s, %s)",
                getJexlExp(keyMetadata),
                getPredicateJexlExp(
                    attributeMatchCondition.getKeyMatchCondition().getMatchCondition()),
                ALL_MATCH_OPERATORS.contains(
                    attributeMatchCondition
                        .getKeyMatchCondition()
                        .getMatchCondition()
                        .getOperator()));
      }
      return MatchCondition.newBuilder()
          .setGenericMatchCondition(
              GenericMatchCondition.newBuilder()
                  .setJexlExpression(JexlExpressionConfig.newBuilder().setJexlExpression(jexlExp)))
          .build();
    }
  }

  private String getPredicateJexlExp(
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

  private String getJexlExp(KeyMetadata keyMetadata) {
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
        throw new IllegalArgumentException("Invalid type in key value condition : " + keyMetadata);
    }
  }

  private MatchOperator getOp(
      KeyMetadata keyMetadata,
      ai.traceable.detection.exclusion.config.service.v1.MatchOperator operator) {
    switch (operator) {
      case MATCH_OPERATOR_EQUALS:
        return CASE_INSENSITIVE_KEY_METADATA_TYPES.contains(keyMetadata)
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

  @Override
  public DetectionExclusionCondition.ConditionCase getConditionCase() {
    return DetectionExclusionCondition.ConditionCase.ATTRIBUTE_MATCH_CONDITION;
  }
}
