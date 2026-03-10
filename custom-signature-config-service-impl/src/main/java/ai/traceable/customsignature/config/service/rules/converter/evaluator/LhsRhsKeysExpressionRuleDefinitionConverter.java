package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.LhsRhsKeysExpression;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.AttributeType;
import ai.traceable.protection.processing.common.v1.StringOperator;
import ai.traceable.protection.processing.common.v1.UserAttributeType;
import ai.traceable.protection.processing.common.v1.utils.ProtoEnumUtils;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import com.google.protobuf.ProtocolMessageEnum;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class LhsRhsKeysExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    LhsRhsKeysExpression expression = clause.getLhsRhsKeysExpression();

    // Add after incorporating attributes to custom signature
    if (expression.hasAttributeLhsExpression() || expression.hasAttributeRhsExpression()) {
      throw new IllegalArgumentException(
          "Attribute-based lhs/rhs expressions are not supported in rule definition conversion");
    }

    MatchExpression lhsMatchExpression =
        expression.hasKeyLhsExpression()
            ? expression.getKeyLhsExpression()
            : expression.getLhsKeyExpression();
    MatchExpression rhsMatchExpression =
        expression.hasKeyRhsExpression()
            ? expression.getKeyRhsExpression()
            : expression.getRhsKeyExpression();

    boolean isLhsAggregation = isAggregationCase(lhsMatchExpression.getMatchKey());
    boolean isRhsAggregation = isAggregationCase(rhsMatchExpression.getMatchKey());

    if (isLhsAggregation != isRhsAggregation) {
      if (isLhsAggregation) {
        return CustomSignatureRuleDefinitionConverterUtils.buildSingleAggregationCondition(
            CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
                getTagEnum(lhsMatchExpression.getMatchKey())),
            CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
                getTagEnum(rhsMatchExpression.getMatchKey())),
            getStringValue(rhsMatchExpression),
            rhsMatchExpression.getMatchOperator(),
            expression.getMatchOperator());
      } else {
        return CustomSignatureRuleDefinitionConverterUtils.buildSingleAggregationCondition(
            CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
                getTagEnum(lhsMatchExpression.getMatchKey())),
            CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
                getTagEnum(rhsMatchExpression.getMatchKey())),
            getStringValue(lhsMatchExpression),
            lhsMatchExpression.getMatchOperator(),
            expression.getMatchOperator());
      }
    }

    if (isLhsAggregation) {
      return CustomSignatureRuleDefinitionConverterUtils.buildLhsRhsAggregationCondition(
          CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
              getTagEnum(lhsMatchExpression.getMatchKey())),
          CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
              getTagEnum(rhsMatchExpression.getMatchKey())),
          getStringValue(lhsMatchExpression),
          getStringValue(rhsMatchExpression),
          lhsMatchExpression.getMatchOperator(),
          rhsMatchExpression.getMatchOperator(),
          expression.getMatchOperator());
    }

    if (isParameterKey(lhsMatchExpression.getMatchKey())
        || isParameterKey(rhsMatchExpression.getMatchKey())) {
      return buildParameterLhsRhsCondition(lhsMatchExpression, rhsMatchExpression, expression);
    }

    if (lhsMatchExpression.getMatchKey() == MatchKey.MATCH_KEY_USER_AGENT
        || rhsMatchExpression.getMatchKey() == MatchKey.MATCH_KEY_USER_AGENT) {
      return buildUserAgentLhsRhsCondition(lhsMatchExpression, rhsMatchExpression, expression);
    }

    String lhsKeyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
            getTagEnum(lhsMatchExpression.getMatchKey()));
    String rhsKeyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
            getTagEnum(rhsMatchExpression.getMatchKey()));

    StringOperator comparisonOperator =
        CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
            expression.getMatchOperator());

    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildStringCondition(
            comparisonOperator,
            lhsKeyPrefix,
            CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
                    lhsMatchExpression.getMatchOperator())
                .getStringOperator(),
            lhsMatchExpression.hasValue()
                ? lhsMatchExpression.getValue().getStringValue()
                : getStringValue(lhsMatchExpression),
            rhsKeyPrefix,
            CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
                    rhsMatchExpression.getMatchOperator())
                .getStringOperator(),
            rhsMatchExpression.hasValue()
                ? rhsMatchExpression.getValue().getStringValue()
                : getStringValue(rhsMatchExpression));

    String evaluationIdentifier = "lhs_rhs_keys-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.LHS_RHS_KEYS_EXPRESSION;
  }

  private static ProtocolMessageEnum getTagEnum(MatchKey matchKey) {
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

  private static boolean isAggregationCase(MatchKey matchKey) {
    return matchKey == MatchKey.MATCH_KEY_QUERY_PARAMS_COUNT
        || matchKey == MatchKey.MATCH_KEY_HEADERS_COUNT
        || matchKey == MatchKey.MATCH_KEY_COOKIES_COUNT;
  }

  private static boolean isParameterKey(MatchKey matchKey) {
    return matchKey == MatchKey.MATCH_KEY_PARAMETER_NAME
        || matchKey == MatchKey.MATCH_KEY_PARAMETER_VALUE;
  }

  private CustomSignatureRuleDefinition buildParameterLhsRhsCondition(
      MatchExpression lhsMatchExpression,
      MatchExpression rhsMatchExpression,
      LhsRhsKeysExpression expression) {

    List<MatchConditionExpression> conditions = new ArrayList<>();

    boolean lhsIsParameter = isParameterKey(lhsMatchExpression.getMatchKey());
    boolean rhsIsParameter = isParameterKey(rhsMatchExpression.getMatchKey());

    if (lhsIsParameter && rhsIsParameter) {
      // Both are parameters - create 4 combinations: query-query, query-body, body-query, body-body
      conditions.add(
          buildSingleLhsRhsCondition(
              lhsMatchExpression, rhsMatchExpression, expression, true, true)); // query-query
      conditions.add(
          buildSingleLhsRhsCondition(
              lhsMatchExpression, rhsMatchExpression, expression, true, false)); // query-body
      conditions.add(
          buildSingleLhsRhsCondition(
              lhsMatchExpression, rhsMatchExpression, expression, false, true)); // body-query
      conditions.add(
          buildSingleLhsRhsCondition(
              lhsMatchExpression, rhsMatchExpression, expression, false, false)); // body-body
    } else if (lhsIsParameter) {
      // Only LHS is parameter - create 2 conditions: LHS as query + RHS, LHS as body + RHS
      conditions.add(
          buildSingleLhsRhsCondition(
              lhsMatchExpression, rhsMatchExpression, expression, true, false)); // LHS as query
      conditions.add(
          buildSingleLhsRhsCondition(
              lhsMatchExpression, rhsMatchExpression, expression, false, false)); // LHS as body
    } else if (rhsIsParameter) {
      // Only RHS is parameter - create 2 conditions: LHS + RHS as query, LHS + RHS as body
      conditions.add(
          buildSingleLhsRhsCondition(
              lhsMatchExpression, rhsMatchExpression, expression, false, true)); // RHS as query
      conditions.add(
          buildSingleLhsRhsCondition(
              lhsMatchExpression, rhsMatchExpression, expression, false, false)); // RHS as body
    }

    // Combine all conditions with OR logic
    MatchConditionExpression logicalExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildOrLogicalExpression(conditions);

    String evaluationIdentifier = "lhs_rhs_keys-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, logicalExpression);
  }

  private MatchConditionExpression buildSingleLhsRhsCondition(
      MatchExpression lhsMatchExpression,
      MatchExpression rhsMatchExpression,
      LhsRhsKeysExpression expression,
      boolean lhsAsQuery,
      boolean rhsAsQuery) {

    // Determine the actual key prefixes based on the original match keys and query/body flags
    String lhsKeyPrefix = getParameterKeyPrefix(lhsAsQuery);
    String rhsKeyPrefix = getParameterKeyPrefix(rhsAsQuery);

    StringOperator comparisonOperator =
        CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
            expression.getMatchOperator());

    return CustomSignatureRuleDefinitionConverterUtils.buildStringCondition(
        comparisonOperator,
        lhsKeyPrefix,
        CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
                lhsMatchExpression.getMatchOperator())
            .getStringOperator(),
        getStringValue(lhsMatchExpression),
        rhsKeyPrefix,
        CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
                rhsMatchExpression.getMatchOperator())
            .getStringOperator(),
        getStringValue(rhsMatchExpression));
  }

  private static String getParameterKeyPrefix(boolean asQuery) {
    if (asQuery) {
      return CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
          getTagEnum(MatchKey.MATCH_KEY_QUERY_PARAMETER_NAME));
    } else {
      return CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
          getTagEnum(MatchKey.MATCH_KEY_BODY_PARAMETER_NAME));
    }
  }

  private static CustomSignatureRuleDefinition buildUserAgentLhsRhsCondition(
      MatchExpression lhsMatchExpression,
      MatchExpression rhsMatchExpression,
      LhsRhsKeysExpression expression) {

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
            AttributeType.ATTRIBUTE_TYPE_CUSTOM);
    String userAgentKeyMatchValue =
        ProtoEnumUtils.getStringExtension(UserAttributeType.USER_ATTRIBUTE_TYPE_USER_AGENT);

    String otherKeyPrefix;
    MatchExpression otherExpression;
    StringOperator.StringMatchOperator otherStringOperator;

    if (lhsMatchExpression.getMatchKey() == MatchKey.MATCH_KEY_USER_AGENT) {
      otherKeyPrefix =
          CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
              getTagEnum(rhsMatchExpression.getMatchKey()));
      otherExpression = rhsMatchExpression;
      otherStringOperator =
          CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
                  rhsMatchExpression.getMatchOperator())
              .getStringOperator();
    } else {
      otherKeyPrefix =
          CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
              getTagEnum(lhsMatchExpression.getMatchKey()));
      otherExpression = lhsMatchExpression;
      otherStringOperator =
          CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
                  lhsMatchExpression.getMatchOperator())
              .getStringOperator();
    }

    // Create dynamic key match condition comparing user agent value with other key
    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildStringCondition(
            CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
                expression.getMatchOperator()),
            keyPrefix,
            StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ,
            userAgentKeyMatchValue,
            otherKeyPrefix,
            otherStringOperator,
            otherExpression.hasValue()
                ? otherExpression.getValue().getStringValue()
                : getStringValue(otherExpression));

    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        "lhs_rhs_keys-" + UUID.randomUUID().toString().substring(0, 8), conditionExpression);
  }
}
