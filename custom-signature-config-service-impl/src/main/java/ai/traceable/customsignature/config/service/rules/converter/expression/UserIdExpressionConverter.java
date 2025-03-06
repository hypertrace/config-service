package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.edge.decision.converter.utils.ConverterUtils.USER_ID_VALUE_LHS;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildInOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildLikeOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.joinChildConditions;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.UserIdExpression;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition.Builder;
import java.util.ArrayList;
import java.util.List;

public class UserIdExpressionConverter implements CustomSignatureExpressionConverter {

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    final UserIdExpression userIdExpression = clause.getUserIdExpression();
    List<Builder> childMatchConditions = new ArrayList<>();
    if (!userIdExpression.getUserIdsList().isEmpty()) {
      childMatchConditions.add(
          buildInOperatorMatchCondition(USER_ID_VALUE_LHS, userIdExpression.getUserIdsList()));
    }
    if (!userIdExpression.getUserIdRegexesList().isEmpty()) {
      childMatchConditions.add(
          buildLikeOperatorMatchCondition(
              USER_ID_VALUE_LHS, userIdExpression.getUserIdRegexesList()));
    }
    return joinChildConditions(childMatchConditions, userIdExpression.getExclude());
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.USER_ID_EXPRESSION;
  }
}
