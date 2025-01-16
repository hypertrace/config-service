package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DetectionExclusionRuleScopeConditionConverter
    implements DetectionExclusionRuleConditionConverter {

  private static final String PATH_JEXL_EXP = "$s.getPath()";
  private static final AttributeDerivationMapping PATH_ATTRIBUTE =
      AttributeDerivationMapping.newBuilder()
          .setName("lhs")
          .setType(FIELD_TYPE_STR)
          .addRules(
              DerivationRule.newBuilder()
                  .setTransformationConfig(
                      DataTransformationConfig.newBuilder()
                          .setJexlExpression(
                              JexlExpressionConfig.newBuilder().setJexlExpression(PATH_JEXL_EXP))))
          .build();

  @Override
  public MatchCondition buildMatchCondition(
      RequestContext requestContext, DetectionExclusionCondition condition) {
    final ScopeCondition scopeCondition = condition.getScopeCondition();
    switch (scopeCondition.getScopeCase()) {
      case URL_SCOPE:
        return JexlUtils.buildLikeOperatorMatchCondition(
                PATH_ATTRIBUTE, scopeCondition.getUrlScope().getUrlRegexesList())
            .setNegate(scopeCondition.getExclude())
            .build();
      default:
        throw new IllegalArgumentException(
            "Unsupported scope case: " + scopeCondition.getScopeCase());
    }
  }

  @Override
  public DetectionExclusionCondition.ConditionCase getConditionCase() {
    return DetectionExclusionCondition.ConditionCase.SCOPE_CONDITION;
  }
}
