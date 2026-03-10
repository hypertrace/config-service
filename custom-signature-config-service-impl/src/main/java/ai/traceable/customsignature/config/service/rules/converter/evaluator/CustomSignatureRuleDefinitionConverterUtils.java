package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureConditionExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureSecRule;
import ai.traceable.protection.processing.common.v1.*;
import ai.traceable.protection.processing.common.v1.MessageType;
import ai.traceable.protection.processing.common.v1.utils.PrefixBuilder;
import ai.traceable.protection.processor.condition.expression.v1.AggregationMatchCondition;
import ai.traceable.protection.processor.condition.expression.v1.CIDRMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.DynamicKeyMatchCondition;
import ai.traceable.protection.processor.condition.expression.v1.KeyMatchOperand;
import ai.traceable.protection.processor.condition.expression.v1.KeyValueMatchCondition;
import ai.traceable.protection.processor.condition.expression.v1.LeafBooleanExpression;
import ai.traceable.protection.processor.condition.expression.v1.LeafMatchConditionExpression;
import ai.traceable.protection.processor.condition.expression.v1.LogicalConditionExpression;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import ai.traceable.protection.processor.condition.expression.v1.NumberMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.StringListMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.StringMatchOperation;
import ai.traceable.protection.processor.condition.expression.v1.UnaryKeyMatchCondition;
import ai.traceable.protection.processor.condition.expression.v1.ValueMatchOperation;
import com.google.protobuf.ProtocolMessageEnum;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.ArrayUtils;

public class CustomSignatureRuleDefinitionConverterUtils {

  private CustomSignatureRuleDefinitionConverterUtils() {}

  public static CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(
      String evaluationIdentifier, MatchConditionExpression conditionExpression) {

    CustomSignatureConditionExpression conditionExpr =
        CustomSignatureConditionExpression.newBuilder()
            .setConditionExpressionEvaluationIdentifier(evaluationIdentifier)
            .setConditionExpression(conditionExpression)
            .build();

    return CustomSignatureRuleDefinition.newBuilder()
        .setCustomSignatureConditionExpression(conditionExpr)
        .build();
  }

  public static CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(
      String evaluationIdentifier, String secRule) {

    CustomSignatureSecRule secRuleExpr =
        CustomSignatureSecRule.newBuilder()
            .setSecRuleEvaluationIdentifier(evaluationIdentifier)
            .setSecRule(secRule)
            .build();

    return CustomSignatureRuleDefinition.newBuilder()
        .setCustomSignatureSecRule(secRuleExpr)
        .build();
  }

  public static MatchConditionExpression buildOrLogicalExpression(
      List<MatchConditionExpression> childExpressions) {
    return buildLogicalExpression(LogicalOperator.LOGICAL_OPERATOR_OR, childExpressions);
  }

  public static MatchConditionExpression buildAndLogicalExpression(
      List<MatchConditionExpression> childExpressions) {
    return buildLogicalExpression(LogicalOperator.LOGICAL_OPERATOR_AND, childExpressions);
  }

  public static MatchConditionExpression buildLogicalExpression(
      LogicalOperator logicalOperator, List<MatchConditionExpression> childExpressions) {
    if (childExpressions.isEmpty()) {
      return buildBooleanExpression(true);
    }
    if (childExpressions.size() == 1) {
      return childExpressions.get(0);
    }

    LogicalConditionExpression logicalExpr =
        LogicalConditionExpression.newBuilder()
            .setLogicalOperator(logicalOperator)
            .addAllChildExpressions(childExpressions)
            .build();

    return MatchConditionExpression.newBuilder().setLogicalExpression(logicalExpr).build();
  }

  public static MatchConditionExpression buildStringLikeCondition(String keyPrefix, String regex) {
    return buildStringCondition(
        keyPrefix, StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_LIKE, false, regex);
  }

  public static String joinRegexes(List<String> regexes) {
    return String.join("|", regexes);
  }

  public static MatchConditionExpression buildStringCondition(
      String keyPrefix,
      StringOperator.StringMatchOperator stringOperator,
      boolean ignoreCase,
      String stringValue) {
    UnaryKeyMatchCondition unaryKeyCondition =
        UnaryKeyMatchCondition.newBuilder()
            .setKeyCondition(
                KeyMatchOperand.newBuilder()
                    .setKeyMetadata(
                        KeyMatchOperand.KeyMetadata.newBuilder()
                            .setFullyQualifiedKeyPrefix(keyPrefix)
                            .build())
                    .setKeyMatchOperation(
                        ValueMatchOperation.newBuilder()
                            .setStringMatchOperation(
                                StringMatchOperation.newBuilder()
                                    .setStringOperator(
                                        StringOperator.newBuilder()
                                            .setStringOperator(stringOperator)
                                            .setIgnoreCase(ignoreCase)
                                            .build())
                                    .setStringValue(stringValue)
                                    .build())
                            .build())
                    .build())
            .build();

    LeafMatchConditionExpression leafExpr =
        LeafMatchConditionExpression.newBuilder()
            .setUnaryKeyMatchCondition(unaryKeyCondition)
            .build();

    return MatchConditionExpression.newBuilder().setLeafMatchConditionExpression(leafExpr).build();
  }

  public static MatchConditionExpression buildStringCondition(
      String keyPrefix,
      StringOperator.StringMatchOperator keyStringOperator,
      String keyStringValue,
      StringOperator.StringMatchOperator valueStringOperator,
      String valueStringValue) {
    KeyMatchOperand keyOperand =
        KeyMatchOperand.newBuilder()
            .setKeyMetadata(
                KeyMatchOperand.KeyMetadata.newBuilder()
                    .setFullyQualifiedKeyPrefix(keyPrefix)
                    .build())
            .setKeyMatchOperation(
                ValueMatchOperation.newBuilder()
                    .setStringMatchOperation(
                        StringMatchOperation.newBuilder()
                            .setStringOperator(
                                StringOperator.newBuilder()
                                    .setStringOperator(keyStringOperator)
                                    .setIgnoreCase(false)
                                    .build())
                            .setStringValue(keyStringValue)
                            .build())
                    .build())
            .build();

    ValueMatchOperation valueOperation =
        ValueMatchOperation.newBuilder()
            .setStringMatchOperation(
                StringMatchOperation.newBuilder()
                    .setStringOperator(
                        StringOperator.newBuilder()
                            .setStringOperator(valueStringOperator)
                            .setIgnoreCase(false)
                            .build())
                    .setStringValue(valueStringValue)
                    .build())
            .build();

    KeyValueMatchCondition keyValueCondition =
        KeyValueMatchCondition.newBuilder()
            .setLhsKeyOperand(keyOperand)
            .setRhsValueMatchOperation(valueOperation)
            .build();

    LeafMatchConditionExpression leafExpr =
        LeafMatchConditionExpression.newBuilder()
            .setKeyValueMatchCondition(keyValueCondition)
            .build();

    return MatchConditionExpression.newBuilder().setLeafMatchConditionExpression(leafExpr).build();
  }

  public static MatchConditionExpression buildStringCondition(
      StringOperator stringOperator,
      String lhsKeyPrefix,
      StringOperator.StringMatchOperator lhsStringOperator,
      String lhsStringValue,
      String rhsKeyPrefix,
      StringOperator.StringMatchOperator rhsStringOperator,
      String rhsStringValue) {
    KeyMatchOperand lhsKeyOperand =
        KeyMatchOperand.newBuilder()
            .setKeyMetadata(
                KeyMatchOperand.KeyMetadata.newBuilder()
                    .setFullyQualifiedKeyPrefix(lhsKeyPrefix)
                    .build())
            .setKeyMatchOperation(
                ValueMatchOperation.newBuilder()
                    .setStringMatchOperation(
                        StringMatchOperation.newBuilder()
                            .setStringOperator(
                                StringOperator.newBuilder()
                                    .setStringOperator(lhsStringOperator)
                                    .setIgnoreCase(false)
                                    .build())
                            .setStringValue(lhsStringValue)
                            .build())
                    .build())
            .build();

    KeyMatchOperand rhsKeyOperand =
        KeyMatchOperand.newBuilder()
            .setKeyMetadata(
                KeyMatchOperand.KeyMetadata.newBuilder()
                    .setFullyQualifiedKeyPrefix(rhsKeyPrefix)
                    .build())
            .setKeyMatchOperation(
                ValueMatchOperation.newBuilder()
                    .setStringMatchOperation(
                        StringMatchOperation.newBuilder()
                            .setStringOperator(
                                StringOperator.newBuilder()
                                    .setStringOperator(rhsStringOperator)
                                    .setIgnoreCase(false)
                                    .build())
                            .setStringValue(rhsStringValue)
                            .build())
                    .build())
            .build();

    DynamicKeyMatchCondition dynamicKeyMatchCondition =
        DynamicKeyMatchCondition.newBuilder()
            .setLhsKeyOperand(lhsKeyOperand)
            .setRhsKeyOperand(rhsKeyOperand)
            .setStringOperator(stringOperator)
            .build();

    LeafMatchConditionExpression leafExpr =
        LeafMatchConditionExpression.newBuilder()
            .setDynamicKeyMatchCondition(dynamicKeyMatchCondition)
            .build();

    return MatchConditionExpression.newBuilder().setLeafMatchConditionExpression(leafExpr).build();
  }

  public static StringOperator matchOperatorToStringOperator(MatchOperator matchOperator) {
    StringOperator.StringMatchOperator stringMatchOperator;
    switch (matchOperator) {
      case MATCH_OPERATOR_EQUALS:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ;
        break;
      case MATCH_OPERATOR_NOT_EQUAL:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_NOT_EQ;
        break;
      case MATCH_OPERATOR_CONTAINS:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_CONTAINS;
        break;
      case MATCH_OPERATOR_NOT_CONTAIN:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_NOT_CONTAINS;
        break;
      case MATCH_OPERATOR_MATCHES_REGEX:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_LIKE;
        break;
      case MATCH_OPERATOR_NOT_MATCH_REGEX:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_NOT_LIKE;
        break;
      case MATCH_OPERATOR_UNSPECIFIED:
      default:
        stringMatchOperator = StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_UNSPECIFIED;
        break;
    }

    return StringOperator.newBuilder()
        .setStringOperator(stringMatchOperator)
        .setIgnoreCase(false)
        .build();
  }

  public static MatchConditionExpression buildStringListAnyEqualsCondition(
      String keyPrefix, List<String> stringValues) {
    ValueMatchOperation valueOperation =
        ValueMatchOperation.newBuilder()
            .setStringListMatchOperation(
                StringListMatchOperation.newBuilder()
                    .setStringOperator(
                        StringOperator.newBuilder()
                            .setStringOperator(
                                StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ)
                            .setIgnoreCase(false)
                            .build())
                    .setMultiMatchOperator(MultiMatchOperator.MULTI_MATCH_OPERATOR_ANY)
                    .addAllStringValues(stringValues)
                    .build())
            .build();

    KeyMatchOperand keyOperand =
        KeyMatchOperand.newBuilder()
            .setKeyMetadata(
                KeyMatchOperand.KeyMetadata.newBuilder()
                    .setFullyQualifiedKeyPrefix(keyPrefix)
                    .build())
            .setKeyMatchOperation(valueOperation)
            .build();

    UnaryKeyMatchCondition unaryKeyMatchCondition =
        UnaryKeyMatchCondition.newBuilder().setKeyCondition(keyOperand).build();

    LeafMatchConditionExpression leafExpr =
        LeafMatchConditionExpression.newBuilder()
            .setUnaryKeyMatchCondition(unaryKeyMatchCondition)
            .build();

    return MatchConditionExpression.newBuilder().setLeafMatchConditionExpression(leafExpr).build();
  }

  public static MatchConditionExpression buildStringKeyValueListAnyEqualsCondition(
      String keyPrefix,
      StringOperator.StringMatchOperator keyStringOperator,
      String keyStringValue,
      StringOperator.StringMatchOperator valueStringOperator,
      List<String> stringValues) {
    ValueMatchOperation keyMatchOperation =
        ValueMatchOperation.newBuilder()
            .setStringMatchOperation(
                StringMatchOperation.newBuilder()
                    .setStringOperator(
                        StringOperator.newBuilder()
                            .setStringOperator(keyStringOperator)
                            .setIgnoreCase(false)
                            .build())
                    .setStringValue(keyStringValue)
                    .build())
            .build();

    ValueMatchOperation valueMatchOperation =
        ValueMatchOperation.newBuilder()
            .setStringListMatchOperation(
                StringListMatchOperation.newBuilder()
                    .setStringOperator(
                        StringOperator.newBuilder()
                            .setStringOperator(valueStringOperator)
                            .setIgnoreCase(false)
                            .build())
                    .setMultiMatchOperator(MultiMatchOperator.MULTI_MATCH_OPERATOR_ANY)
                    .addAllStringValues(stringValues)
                    .build())
            .build();

    KeyValueMatchCondition keyValueCondition =
        KeyValueMatchCondition.newBuilder()
            .setLhsKeyOperand(
                KeyMatchOperand.newBuilder()
                    .setKeyMetadata(
                        KeyMatchOperand.KeyMetadata.newBuilder()
                            .setFullyQualifiedKeyPrefix(keyPrefix)
                            .build())
                    .setKeyMatchOperation(keyMatchOperation)
                    .build())
            .setRhsValueMatchOperation(valueMatchOperation)
            .build();

    LeafMatchConditionExpression leafExpr =
        LeafMatchConditionExpression.newBuilder()
            .setKeyValueMatchCondition(keyValueCondition)
            .build();

    return MatchConditionExpression.newBuilder().setLeafMatchConditionExpression(leafExpr).build();
  }

  public static MatchConditionExpression buildStringKeyValueLikeCondition(
      String keyPrefix,
      StringOperator.StringMatchOperator keyStringOperator,
      String keyStringValue,
      String regex) {
    return buildStringCondition(
        keyPrefix,
        keyStringOperator,
        keyStringValue,
        StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_LIKE,
        regex);
  }

  public static MatchConditionExpression buildNegateExpression(
      MatchConditionExpression expression) {
    return MatchConditionExpression.newBuilder().setNegateExpression(expression).build();
  }

  public static MatchConditionExpression buildBooleanExpression(boolean value) {
    return MatchConditionExpression.newBuilder()
        .setLeafBooleanExpression(LeafBooleanExpression.newBuilder().setValue(value).build())
        .build();
  }

  public static MatchConditionExpression buildCidrMatchCondition(
      String keyPrefix, List<String> cidrBlocks) {
    KeyMatchOperand keyOperand =
        KeyMatchOperand.newBuilder()
            .setKeyMetadata(
                KeyMatchOperand.KeyMetadata.newBuilder()
                    .setFullyQualifiedKeyPrefix(keyPrefix)
                    .build())
            .build();

    ValueMatchOperation valueOperation =
        ValueMatchOperation.newBuilder()
            .setCidrMatchOperation(
                CIDRMatchOperation.newBuilder().addAllCidrBlocks(cidrBlocks).build())
            .build();

    KeyValueMatchCondition keyValueCondition =
        KeyValueMatchCondition.newBuilder()
            .setLhsKeyOperand(keyOperand)
            .setRhsValueMatchOperation(valueOperation)
            .build();

    LeafMatchConditionExpression leafExpr =
        LeafMatchConditionExpression.newBuilder()
            .setKeyValueMatchCondition(keyValueCondition)
            .build();

    return MatchConditionExpression.newBuilder().setLeafMatchConditionExpression(leafExpr).build();
  }

  public static CustomSignatureRuleDefinition buildSingleAggregationCondition(
      String aggregationKeyPrefix,
      String regularKeyPrefix,
      String regularValue,
      MatchOperator regularMatchOperator,
      MatchOperator comparisonOperator) {

    AggregationMatchCondition.AggregationOperand aggregationOperand =
        AggregationMatchCondition.AggregationOperand.newBuilder()
            .setKeyCondition(
                KeyMatchOperand.newBuilder()
                    .setKeyMetadata(
                        KeyMatchOperand.KeyMetadata.newBuilder()
                            .setFullyQualifiedKeyPrefix(aggregationKeyPrefix)
                            .build())
                    .build())
            .setKeyAggregationOperator(AggregationOperator.AGGREGATION_OPERATOR_COUNT)
            .build();

    AggregationMatchCondition.AggregationOperand keyMatchOperand =
        AggregationMatchCondition.AggregationOperand.newBuilder()
            .setKeyCondition(
                KeyMatchOperand.newBuilder()
                    .setKeyMetadata(
                        KeyMatchOperand.KeyMetadata.newBuilder()
                            .setFullyQualifiedKeyPrefix(regularKeyPrefix)
                            .build())
                    .setKeyMatchOperation(
                        ValueMatchOperation.newBuilder()
                            .setStringMatchOperation(
                                StringMatchOperation.newBuilder()
                                    .setStringOperator(
                                        matchOperatorToStringOperator(regularMatchOperator))
                                    .setStringValue(regularValue)
                                    .build())
                            .build())
                    .build())
            .build();

    AggregationMatchCondition aggregationCondition =
        AggregationMatchCondition.newBuilder()
            .setLhsOperand(aggregationOperand)
            .setRhsAggregationMatchOperation(
                AggregationMatchCondition.AggregationNumberMatchOperation.newBuilder()
                    .setOperand(keyMatchOperand)
                    .setNumberOperator(convertMatchOperatorToNumberOperator(comparisonOperator))
                    .build())
            .build();

    MatchConditionExpression conditionExpression =
        MatchConditionExpression.newBuilder()
            .setLeafMatchConditionExpression(
                LeafMatchConditionExpression.newBuilder()
                    .setAggregationMatchCondition(aggregationCondition)
                    .setEmitMatchedAttribute(false))
            .build();

    String evaluationIdentifier = "lhs_rhs_keys-" + UUID.randomUUID().toString().substring(0, 8);
    return buildCustomSignatureRuleDefinition(evaluationIdentifier, conditionExpression);
  }

  public static CustomSignatureRuleDefinition buildLhsRhsAggregationCondition(
      String lhsKeyPrefix,
      String rhsKeyPrefix,
      String lhsValue,
      String rhsValue,
      MatchOperator lhsMatchOperator,
      MatchOperator rhsMatchOperator,
      MatchOperator comparisonOperator) {

    AggregationMatchCondition.AggregationOperand lhsOperand =
        AggregationMatchCondition.AggregationOperand.newBuilder()
            .setKeyCondition(
                KeyMatchOperand.newBuilder()
                    .setKeyMetadata(
                        KeyMatchOperand.KeyMetadata.newBuilder()
                            .setFullyQualifiedKeyPrefix(lhsKeyPrefix)
                            .build())
                    .setKeyMatchOperation(
                        ValueMatchOperation.newBuilder()
                            .setStringMatchOperation(
                                StringMatchOperation.newBuilder()
                                    .setStringOperator(
                                        matchOperatorToStringOperator(lhsMatchOperator))
                                    .setStringValue(lhsValue)
                                    .build())
                            .build())
                    .build())
            .setKeyAggregationOperator(AggregationOperator.AGGREGATION_OPERATOR_COUNT)
            .build();

    AggregationMatchCondition.AggregationOperand rhsOperand =
        AggregationMatchCondition.AggregationOperand.newBuilder()
            .setKeyCondition(
                KeyMatchOperand.newBuilder()
                    .setKeyMetadata(
                        KeyMatchOperand.KeyMetadata.newBuilder()
                            .setFullyQualifiedKeyPrefix(rhsKeyPrefix)
                            .build())
                    .setKeyMatchOperation(
                        ValueMatchOperation.newBuilder()
                            .setStringMatchOperation(
                                StringMatchOperation.newBuilder()
                                    .setStringOperator(
                                        matchOperatorToStringOperator(rhsMatchOperator))
                                    .setStringValue(rhsValue)
                                    .build())
                            .build())
                    .build())
            .setKeyAggregationOperator(AggregationOperator.AGGREGATION_OPERATOR_COUNT)
            .build();

    AggregationMatchCondition aggregationCondition =
        AggregationMatchCondition.newBuilder()
            .setLhsOperand(lhsOperand)
            .setRhsAggregationMatchOperation(
                AggregationMatchCondition.AggregationNumberMatchOperation.newBuilder()
                    .setOperand(rhsOperand)
                    .setNumberOperator(convertMatchOperatorToNumberOperator(comparisonOperator))
                    .build())
            .build();

    MatchConditionExpression conditionExpression =
        MatchConditionExpression.newBuilder()
            .setLeafMatchConditionExpression(
                LeafMatchConditionExpression.newBuilder()
                    .setAggregationMatchCondition(aggregationCondition)
                    .setEmitMatchedAttribute(false))
            .build();

    String evaluationIdentifier = "lhs_rhs_keys-" + UUID.randomUUID().toString().substring(0, 8);
    return buildCustomSignatureRuleDefinition(evaluationIdentifier, conditionExpression);
  }

  public static CustomSignatureRuleDefinition buildAggregationCondition(
      String keyPrefix, MatchOperator matchOperator, double matchValue) {

    AggregationMatchCondition aggregationCondition =
        AggregationMatchCondition.newBuilder()
            .setLhsOperand(
                AggregationMatchCondition.AggregationOperand.newBuilder()
                    .setKeyCondition(
                        KeyMatchOperand.newBuilder()
                            .setKeyMetadata(
                                KeyMatchOperand.KeyMetadata.newBuilder()
                                    .setFullyQualifiedKeyPrefix(keyPrefix)
                                    .build())
                            .build())
                    .setKeyAggregationOperator(AggregationOperator.AGGREGATION_OPERATOR_COUNT)
                    .build())
            .setRhsNumberMatchOperation(
                NumberMatchOperation.newBuilder()
                    .setNumberOperator(convertMatchOperatorToNumberOperator(matchOperator))
                    .setNumberValue(matchValue)
                    .build())
            .build();

    MatchConditionExpression conditionExpression =
        MatchConditionExpression.newBuilder()
            .setLeafMatchConditionExpression(
                LeafMatchConditionExpression.newBuilder()
                    .setAggregationMatchCondition(aggregationCondition)
                    .setEmitMatchedAttribute(false))
            .build();

    String evaluationIdentifier =
        "match_expression-" + UUID.randomUUID().toString().substring(0, 8);
    return buildCustomSignatureRuleDefinition(evaluationIdentifier, conditionExpression);
  }

  private static NumberMatchOperator convertMatchOperatorToNumberOperator(
      MatchOperator matchOperator) {
    switch (matchOperator) {
      case MATCH_OPERATOR_EQUALS:
        return NumberMatchOperator.NUMBER_MATCH_OPERATOR_EQ;
      case MATCH_OPERATOR_NOT_EQUAL:
        return NumberMatchOperator.NUMBER_MATCH_OPERATOR_NOT_EQ;
      case MATCH_OPERATOR_GREATER_THAN:
        return NumberMatchOperator.NUMBER_MATCH_OPERATOR_GT;
      case MATCH_OPERATOR_LESS_THAN:
        return NumberMatchOperator.NUMBER_MATCH_OPERATOR_LT;
      default:
        throw new IllegalArgumentException(
            "Unsupported match operator for aggregation: " + matchOperator);
    }
  }

  public static String getKeyRequestPrefix(ProtocolMessageEnum... enumValues) {
    ProtocolMessageEnum[] allEnums =
        ArrayUtils.addAll(new ProtocolMessageEnum[] {MessageType.MESSAGE_TYPE_REQUEST}, enumValues);
    return PrefixBuilder.buildAppendablePrefix(allEnums);
  }

  public static String getKeyIpPrefix(ProtocolMessageEnum... enumValues) {
    ProtocolMessageEnum[] allEnums =
        ArrayUtils.addAll(new ProtocolMessageEnum[] {MessageType.MESSAGE_TYPE_IP}, enumValues);
    return PrefixBuilder.buildAppendablePrefix(allEnums);
  }
}
