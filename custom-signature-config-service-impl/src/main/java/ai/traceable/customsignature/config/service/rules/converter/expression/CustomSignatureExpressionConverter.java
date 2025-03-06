package ai.traceable.customsignature.config.service.rules.converter.expression;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;

public interface CustomSignatureExpressionConverter {

  MatchCondition buildMatchCondition(Clause clause);

  Clause.ClauseCase getClauseCase();
}
