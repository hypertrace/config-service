package ai.traceable.ratelimiting.service.v2.rules.converter.condition;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase.SCOPE_CONDITION;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.edge.decision.config.service.v1.BinaryOperator;
import ai.traceable.edge.decision.config.service.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.v1.MatchOperator;
import ai.traceable.edge.decision.config.service.v1.StructuredMatchCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition.ConditionCase;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;

public class RateLimitingScopeConditionConverter implements RateLimitingConditionConverter {

  @Override
  public MatchCondition buildMatchCondition(final LeafCondition leafCondition) {
    final ScopeCondition scopeCondition = leafCondition.getScopeCondition();
    final StructuredMatchCondition.Builder builder =
        StructuredMatchCondition.newBuilder()
            .setLhs(
                AttributeDerivationMapping.newBuilder()
                    .setName("lhs")
                    .setType(FIELD_TYPE_STR)
                    .addRules(
                        DerivationRule.newBuilder()
                            .setTransformationConfig(
                                DataTransformationConfig.newBuilder()
                                    .setJexlExpression(
                                        JexlExpressionConfig.newBuilder()
                                            .setJexlExpression("$s.getPath()")))));
    String regexes = null;
    switch (scopeCondition.getScopeCase()) {
      case URL_SCOPE:
        regexes = String.join("|", scopeCondition.getUrlScope().getUrlRegexesList());
        break;
      case ENTITY_SCOPE:
      case LABEL_SCOPE:
      default:
        throw new IllegalArgumentException("Unknown scope case: " + scopeCondition.getScopeCase());
    }
    builder.setBinaryOperator(
        BinaryOperator.newBuilder()
            .setMatchOperator(MatchOperator.MATCH_OPERATOR_LIKE)
            .setRegex(regexes));
    return MatchCondition.newBuilder().setStructuredMatchCondition(builder).build();
  }

  @Override
  public ConditionCase getConditionCase() {
    return SCOPE_CONDITION;
  }
}
