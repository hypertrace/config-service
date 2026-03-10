package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.EmailDomainExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.AttributeType;
import ai.traceable.protection.processing.common.v1.StringOperator;
import ai.traceable.protection.processing.common.v1.UserAttributeType;
import ai.traceable.protection.processing.common.v1.utils.ProtoEnumUtils;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class EmailDomainExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    EmailDomainExpression emailDomainExpression = clause.getEmailDomainExpression();

    List<MatchConditionExpression> childExpressions = new ArrayList<>();

    String keyPrefix =
        CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
            AttributeType.ATTRIBUTE_TYPE_CUSTOM);
    String keyMatchValue =
        ProtoEnumUtils.getStringExtension(UserAttributeType.USER_ATTRIBUTE_TYPE_USER_ID);

    if (!emailDomainExpression.getEmailDomainsList().isEmpty()) {
      childExpressions.add(
          CustomSignatureRuleDefinitionConverterUtils.buildStringKeyValueListAnyEqualsCondition(
              keyPrefix,
              StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ,
              keyMatchValue,
              StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_CONTAINS,
              emailDomainExpression.getEmailDomainsList()));
    }

    if (!emailDomainExpression.getEmailRegexesList().isEmpty()) {
      String joinedRegex =
          CustomSignatureRuleDefinitionConverterUtils.joinRegexes(
              emailDomainExpression.getEmailRegexesList());
      childExpressions.add(
          CustomSignatureRuleDefinitionConverterUtils.buildStringKeyValueLikeCondition(
              keyPrefix,
              StringOperator.StringMatchOperator.STRING_MATCH_OPERATOR_EQ,
              keyMatchValue,
              joinedRegex));
    }

    if (childExpressions.isEmpty()) {
      throw new IllegalArgumentException(
          "No email domains or email regexes present in email domain expression");
    }

    MatchConditionExpression conditionExpression =
        CustomSignatureRuleDefinitionConverterUtils.buildOrLogicalExpression(childExpressions);

    if (emailDomainExpression.getExclude()) {
      conditionExpression =
          CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(conditionExpression);
    }

    String evaluationIdentifier = "email_domain-" + UUID.randomUUID().toString().substring(0, 8);
    return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
        evaluationIdentifier, conditionExpression);
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.EMAIL_DOMAIN_EXPRESSION;
  }
}
