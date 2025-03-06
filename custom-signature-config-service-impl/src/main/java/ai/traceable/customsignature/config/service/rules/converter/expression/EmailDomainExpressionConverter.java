package ai.traceable.customsignature.config.service.rules.converter.expression;

import static ai.traceable.edge.decision.converter.utils.ConverterUtils.USER_ID_VALUE_LHS;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildContainsOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.buildLikeOperatorMatchCondition;
import static ai.traceable.edge.decision.converter.utils.ConverterUtils.joinChildConditions;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.EmailDomainExpression;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import java.util.ArrayList;
import java.util.List;

public class EmailDomainExpressionConverter implements CustomSignatureExpressionConverter {

  @Override
  public MatchCondition buildMatchCondition(final Clause clause) {
    final EmailDomainExpression emailDomainExpression = clause.getEmailDomainExpression();
    List<MatchCondition.Builder> childMatchConditions = new ArrayList<>();
    if (!emailDomainExpression.getEmailDomainsList().isEmpty()) {
      childMatchConditions.addAll(
          buildContainsOperatorMatchCondition(
              USER_ID_VALUE_LHS, emailDomainExpression.getEmailDomainsList()));
    }
    if (!emailDomainExpression.getEmailRegexesList().isEmpty()) {
      childMatchConditions.add(
          buildLikeOperatorMatchCondition(
              USER_ID_VALUE_LHS, emailDomainExpression.getEmailRegexesList()));
    }
    return joinChildConditions(childMatchConditions, emailDomainExpression.getExclude());
  }

  @Override
  public Clause.ClauseCase getClauseCase() {
    return Clause.ClauseCase.EMAIL_DOMAIN_EXPRESSION;
  }
}
