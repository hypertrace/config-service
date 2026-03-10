package ai.traceable.customsignature.config.service.rules.converter.evaluator;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import ai.traceable.protection.engine.config.customsignature.v1.CustomSignatureRuleDefinition;
import ai.traceable.protection.processing.common.v1.AttributeType;
import ai.traceable.protection.processing.common.v1.ScopeAttributeType;
import ai.traceable.protection.processing.common.v1.ScopeType;
import ai.traceable.protection.processor.condition.expression.v1.MatchConditionExpression;
import com.google.inject.Inject;
import java.util.UUID;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ScopeExpressionRuleDefinitionConverter
    implements CustomSignatureRuleDefinitionConverter {

  @Override
  public CustomSignatureRuleDefinition buildCustomSignatureRuleDefinition(Clause clause) {
    ScopeExpression scopeExpression = clause.getScopeExpression();
    MatchConditionExpression conditionExpression;
    switch (scopeExpression.getScopeCase()) {
      case URL_SCOPE:
        if (scopeExpression.getUrlScope().getUrlRegexesList().isEmpty()) {
          throw new IllegalArgumentException("No url regexes present in url scope");
        }
        String keyPrefix =
            CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
                AttributeType.ATTRIBUTE_TYPE_URL);
        String joinedRegex =
            CustomSignatureRuleDefinitionConverterUtils.joinRegexes(
                scopeExpression.getUrlScope().getUrlRegexesList());
        conditionExpression =
            CustomSignatureRuleDefinitionConverterUtils.buildStringLikeCondition(
                keyPrefix, joinedRegex);
        if (scopeExpression.getExclude()) {
          conditionExpression =
              CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(
                  conditionExpression);
        }

        String evaluationIdentifier = "scope-" + UUID.randomUUID().toString().substring(0, 8);
        return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
            evaluationIdentifier, conditionExpression);
      case ENTITY_SCOPE:
        ScopeExpression.EntityScope entityScope = scopeExpression.getEntityScope();
        switch (entityScope.getEntityType()) {
          case ENTITY_TYPE_API:
            MatchConditionExpression apiCondition =
                CustomSignatureRuleDefinitionConverterUtils.buildStringListAnyEqualsCondition(
                    CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
                        ScopeType.SCOPE_TYPE_API, ScopeAttributeType.SCOPE_ATTRIBUTE_TYPE_ID),
                    entityScope.getEntityIdsList());
            if (scopeExpression.getExclude()) {
              apiCondition =
                  CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(apiCondition);
            }
            String apiEvaluationIdentifier =
                "scope-" + UUID.randomUUID().toString().substring(0, 8);
            return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
                apiEvaluationIdentifier, apiCondition);

          case ENTITY_TYPE_SERVICE:
            MatchConditionExpression serviceCondition =
                CustomSignatureRuleDefinitionConverterUtils.buildStringListAnyEqualsCondition(
                    CustomSignatureRuleDefinitionConverterUtils.getKeyRequestPrefix(
                        ScopeType.SCOPE_TYPE_SERVICE, ScopeAttributeType.SCOPE_ATTRIBUTE_TYPE_ID),
                    entityScope.getEntityIdsList());
            if (scopeExpression.getExclude()) {
              serviceCondition =
                  CustomSignatureRuleDefinitionConverterUtils.buildNegateExpression(
                      serviceCondition);
            }
            String serviceEvaluationIdentifier =
                "scope-" + UUID.randomUUID().toString().substring(0, 8);
            return CustomSignatureRuleDefinitionConverterUtils.buildCustomSignatureRuleDefinition(
                serviceEvaluationIdentifier, serviceCondition);

          default:
            throw new IllegalArgumentException(
                "Unsupported entity type case: " + entityScope.getEntityType());
        }
      // TODO: Implement label scope case when label-based scoping is supported
      case LABEL_SCOPE:

      case SCOPE_NOT_SET:
      default:
        throw new IllegalArgumentException("Unknown scope case: " + scopeExpression.getScopeCase());
    }
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.SCOPE_EXPRESSION;
  }
}
