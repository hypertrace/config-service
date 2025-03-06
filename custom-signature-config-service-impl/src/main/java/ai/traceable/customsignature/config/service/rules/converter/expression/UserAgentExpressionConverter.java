package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.edge.decision.converter.utils.ConverterUtils.USER_AGENT_LHS;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildInOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildLikeOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.joinChildConditions;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.Clause.ClauseCase;
import ai.traceable.customsignature.config.service.v1.UserAgentExpression;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition.Builder;
import java.util.ArrayList;
import java.util.List;

public class UserAgentExpressionConverter implements CustomSignatureExpressionConverter {

  @Override
  public MatchCondition buildMatchCondition(Clause clause) {
    UserAgentExpression userAgentExpression = clause.getUserAgentExpression();
    List<Builder> childMatchConditions = new ArrayList<>();
    if (!userAgentExpression.getUserAgentsList().isEmpty()) {
      childMatchConditions.add(
          buildInOperatorMatchCondition(USER_AGENT_LHS, userAgentExpression.getUserAgentsList()));
    }
    if (!userAgentExpression.getUserAgentRegexesList().isEmpty()) {
      childMatchConditions.add(
          buildLikeOperatorMatchCondition(
              USER_AGENT_LHS, userAgentExpression.getUserAgentRegexesList()));
    }
    return joinChildConditions(childMatchConditions, userAgentExpression.getExclude());
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return ClauseCase.USER_AGENT_EXPRESSION;
  }
}
