package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.KeyValueExpression;
import ai.traceable.customsignature.config.service.v1.KeyValueTag;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.AttributeType;
import ai.traceable.protection.processing.common.v1.StringOperator;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import com.google.protobuf.ProtocolMessageEnum;
import java.util.List;
import java.util.UUID;

public class KeyValueExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    KeyValueExpression keyValueExpression = clause.getKeyValueExpression();

    if (keyValueExpression.getMatchCategory() != MatchCategory.MATCH_CATEGORY_REQUEST) {
      throw new IllegalArgumentException(
          "Invalid match category: " + keyValueExpression.getMatchCategory());
    }

    if (keyValueExpression.getTag() == KeyValueTag.KEY_VALUE_TAG_PARAMETER) {
      return buildParameterMatchCondition(keyValueExpression);
    }

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
            getAttributeTypeString(keyValueExpression.getTag()));

    StringOperator keyStringOperator =
        CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
            keyValueExpression.getKeyMatchOperator());
    StringOperator valueStringOperator =
        CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
            keyValueExpression.getValueMatchOperator());

    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildStringCondition(
            keyPrefix,
            keyStringOperator.getStringOperator(),
            keyValueExpression.getMatchKey(),
            valueStringOperator.getStringOperator(),
            keyValueExpression.getMatchValue());

    String evaluationIdentifier = "key_value-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.KEY_VALUE_EXPRESSION;
  }

  private static ProtocolMessageEnum getAttributeTypeString(KeyValueTag tag) {
    switch (tag) {
      case KEY_VALUE_TAG_HEADER:
        return AttributeType.ATTRIBUTE_TYPE_HEADER;
      case KEY_VALUE_TAG_COOKIE:
        return AttributeType.ATTRIBUTE_TYPE_COOKIE;
      case KEY_VALUE_TAG_QUERY_PARAMETER:
        return AttributeType.ATTRIBUTE_TYPE_QUERY_PARAM;
      case KEY_VALUE_TAG_BODY_PARAMETER:
        return AttributeType.ATTRIBUTE_TYPE_BODY_PARAM;
      case KEY_VALUE_TAG_UNSPECIFIED:
      default:
        throw new IllegalArgumentException("Unsupported key value tag: " + tag);
    }
  }

  private CustomSignatureRuleDefinition buildParameterMatchCondition(
      KeyValueExpression keyValueExpression) {
    String queryParamKeyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
            getAttributeTypeString(KeyValueTag.KEY_VALUE_TAG_QUERY_PARAMETER));
    MatchConditionExpression queryParamCondition =
        buildStringConditionForTag(keyValueExpression, queryParamKeyPrefix);

    String bodyParamKeyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
            getAttributeTypeString(KeyValueTag.KEY_VALUE_TAG_BODY_PARAMETER));
    MatchConditionExpression bodyParamCondition =
        buildStringConditionForTag(keyValueExpression, bodyParamKeyPrefix);

    List<MatchConditionExpression> conditions = List.of(queryParamCondition, bodyParamCondition);
    MatchConditionExpression logicalExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildOrLogicalExpression(conditions);

    String evaluationIdentifier = "key_value-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, logicalExpression);
  }

  private MatchConditionExpression buildStringConditionForTag(
      KeyValueExpression keyValueExpression, String keyPrefix) {
    StringOperator keyStringOperator =
        CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
            keyValueExpression.getKeyMatchOperator());
    StringOperator valueStringOperator =
        CustomSignatureRuleDefinitionConverterUtils.matchOperatorToStringOperator(
            keyValueExpression.getValueMatchOperator());

    return CustomSignatureRuleDefinitionConverterUtils.buildStringCondition(
        keyPrefix,
        keyStringOperator.getStringOperator(),
        keyValueExpression.getMatchKey(),
        valueStringOperator.getStringOperator(),
        keyValueExpression.getMatchValue());
  }
}
