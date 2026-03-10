package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.AttributeType;
import ai.traceable.protection.processing.common.v1.StringOperator;
import ai.traceable.protection.processing.common.v1.UserAttributeType;
import ai.traceable.protection.processing.common.v1.utils.ProtoEnumUtils;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import com.google.protobuf.ProtocolMessageEnum;
import java.util.List;
import java.util.UUID;

public class MatchExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    MatchExpression matchExpression = clause.getMatchExpression();
    if (matchExpression.getMatchCategory() != MatchCategory.MATCH_CATEGORY_REQUEST) {
      throw new IllegalArgumentException(
          "Invalid match category: " + matchExpression.getMatchCategory());
    }

    // Check if this is an aggregation case
    if (isAggregationCase(matchExpression.getMatchKey())) {
      String keyPrefix =
          CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
              getKeyEnum(matchExpression.getMatchKey()));
      MatchOperator matchOperator = matchExpression.getMatchOperator();
      double matchValue = getNumberValue(matchExpression);
      return CustomSignatureRuleDefinitionConverterUtils.buildAggregationCondition(
          keyPrefix, matchOperator, matchValue);
    }

    String stringValue = getStringValue(matchExpression);
    MatchOperator matchOperator = matchExpression.getMatchOperator();

    MatchConditionExpression conditionExpression;

    if (isParameterKey(matchExpression.getMatchKey())) {
      conditionExpression = buildParameterMatchCondition(stringValue, matchOperator);
    } else if (matchExpression.getMatchKey() == MatchKey.MATCH_KEY_USER_AGENT) {
      String keyPrefix =
          CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
              AttributeType.ATTRIBUTE_TYPE_CUSTOM);
      String keyMatchValue =
          ProtoEnumUtils.getStringExtension(UserAttributeType.USER_ATTRIBUTE_TYPE_USER_AGENT);
      StringOperator.StringMatchOperator stringOperator =
          CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(matchOperator)
              .getStringOperator();

      if (stringOperator == StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_LIKE) {
        conditionExpression =
            CustomSignatureRuleDefinitionConverterUtils.buildStringCondition(
                keyPrefix,
                StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ,
                keyMatchValue,
                stringOperator,
                stringValue);
      } else {
        conditionExpression =
            CustomSignatureRuleDefinitionConverterUtils.buildStringKeyValueListAnyEqualsCondition(
                keyPrefix,
                StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ,
                keyMatchValue,
                stringOperator,
                List.of(stringValue));
      }
    } else {
      String keyPrefix =
          CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
              getKeyEnum(matchExpression.getMatchKey()));
      conditionExpression =
          CustomSignatureRuleDefinitionConverterUtils.buildStringCondition(
              keyPrefix,
              CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
                      matchOperator)
                  .getStringOperator(),
              true,
              stringValue);
    }

    String evaluationIdentifier =
        "match_expression-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.MATCH_EXPRESSION;
  }

  private static String getStringValue(MatchExpression matchExpression) {
    if (matchExpression.hasValue()) {
      if (matchExpression.getValue().hasStringValue()) {
        return matchExpression.getValue().getStringValue();
      } else if (matchExpression.getValue().hasNumberValue()) {
        return String.valueOf(matchExpression.getValue().getNumberValue());
      }
    }
    // Fallback to deprecated match_value if present
    return matchExpression.getMatchValue();
  }

  private static ProtocolMessageEnum getKeyEnum(MatchKey matchKey) {
    switch (matchKey) {
      case MATCH_KEY_URL:
        return AttributeType.ATTRIBUTE_TYPE_URL;
      case MATCH_KEY_HOST:
        return AttributeType.ATTRIBUTE_TYPE_HOST;
      case MATCH_KEY_HTTP_METHOD:
        return AttributeType.ATTRIBUTE_TYPE_METHOD;
      case MATCH_KEY_USER_AGENT:
        return UserAttributeType.USER_ATTRIBUTE_TYPE_USER_AGENT;
      case MATCH_KEY_HEADER_NAME:
      case MATCH_KEY_HEADER_VALUE:
      case MATCH_KEY_HEADERS_COUNT:
        return AttributeType.ATTRIBUTE_TYPE_HEADER;
      case MATCH_KEY_QUERY_PARAMETER_NAME:
      case MATCH_KEY_QUERY_PARAMETER_VALUE:
      case MATCH_KEY_QUERY_PARAMS_COUNT:
        return AttributeType.ATTRIBUTE_TYPE_QUERY_PARAM;
      case MATCH_KEY_BODY_PARAMETER_NAME:
      case MATCH_KEY_BODY_PARAMETER_VALUE:
        return AttributeType.ATTRIBUTE_TYPE_BODY_PARAM;
      case MATCH_KEY_COOKIE_NAME:
      case MATCH_KEY_COOKIE_VALUE:
      case MATCH_KEY_COOKIES_COUNT:
        return AttributeType.ATTRIBUTE_TYPE_COOKIE;
      case MATCH_KEY_BODY:
        return AttributeType.ATTRIBUTE_TYPE_BODY;
      case MATCH_KEY_BODY_SIZE:
        return AttributeType.ATTRIBUTE_TYPE_BODY_SIZE;
      case MATCH_KEY_STATUS_CODE:
        return AttributeType.ATTRIBUTE_TYPE_STATUS_CODE;
      default:
        throw new IllegalArgumentException("Unsupported match key: " + matchKey);
    }
  }

  private static boolean isAggregationCase(MatchKey matchKey) {
    return matchKey == MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT
        || matchKey == MatchKey.MATCH_KEY_HEADERS_COUNT
        || matchKey == MatchKey.MATCH_KEY_COOKIES_COUNT;
  }

  private static double getNumberValue(MatchExpression matchExpression) {
    if (matchExpression.hasValue()) {
      if (matchExpression.getValue().hasNumberValue()) {
        return matchExpression.getValue().getNumberValue();
      } else if (matchExpression.getValue().hasStringValue()) {
        try {
          return Double.parseDouble(matchExpression.getValue().getStringValue());
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Cannot convert string value to number for aggregation: "
                  + matchExpression.getValue().getStringValue());
        }
      }
    }
    // Fallback to deprecated match_value if present
    try {
      return Double.parseDouble(matchExpression.getMatchValue());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Cannot convert string value to number for aggregation: "
              + matchExpression.getMatchValue());
    }
  }

  private static boolean isParameterKey(MatchKey matchKey) {
    return matchKey == MatchKey.MATCH_KEY_PARAMETER_NAME
        || matchKey == MatchKey.MATCH_KEY_PARAMETER_VALUE;
  }

  private MatchConditionExpression buildParameterMatchCondition(
      String stringValue, MatchOperator matchOperator) {

    StringOperator stringOperator =
        CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(matchOperator);

    String queryKeyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
            getKeyEnum(MatchKey.MATCH_KEY_QUERY_PARAMETER_NAME));

    MatchConditionExpression queryCondition =
        CustomSignatureRuleDefinitionConverterUtils.buildStringCondition(
            queryKeyPrefix, stringOperator.getStringOperator(), true, stringValue);

    String bodyKeyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
            getKeyEnum(MatchKey.MATCH_KEY_BODY_PARAMETER_NAME));

    MatchConditionExpression bodyCondition =
        CustomSignatureRuleDefinitionConverterUtils.buildStringCondition(
            bodyKeyPrefix, stringOperator.getStringOperator(), true, stringValue);

    List<MatchConditionExpression> conditions = List.of(queryCondition, bodyCondition);
    return CustomSignatureRuleDefinitionConverterUtils.buildOrLogicalExpression(conditions);
  }
}
