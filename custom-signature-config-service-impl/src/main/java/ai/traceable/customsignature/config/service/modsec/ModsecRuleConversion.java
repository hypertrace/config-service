package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.customsignature.config.service.modsec.registry.ModsecActions;
import ai.traceable.customsignature.config.service.modsec.registry.ModsecRuleMappings;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

public class ModsecRuleConversion {
  static final String NEW_LINE_DELIMITER = "\n";

  private final ModsecRuleMappings modsecRuleMappings;

  @Inject
  public ModsecRuleConversion(ModsecRuleMappings modsecRuleMappings) {
    this.modsecRuleMappings = modsecRuleMappings;
  }

  public String getModsecRuleForANDClauses(List<Clause> clauses, ModsecActions modsecActions) {
    if (clauses.isEmpty()) {
      return "";
    }
    int size = clauses.size();
    boolean responsePhase = clauses.stream().anyMatch(this::hasResponseVariable);
    if (size == 1) {
      return modsecRuleMappings.getModsecRule(
          getVariableString(clauses.get(0)),
          getOperatorString(clauses.get(0)),
          modsecActions.getSingularRuleActionsString(responsePhase));
    }

    List<String> modsecRules = new ArrayList<>();
    modsecRules.add(
        modsecRuleMappings.getModsecRule(
            getVariableString(clauses.get(0)),
            getOperatorString(clauses.get(0)),
            modsecActions.getChainedRulePrimaryActionsString(responsePhase)));
    for (int i = 1; i < size - 1; i++) {
      modsecRules.add(
          modsecRuleMappings.getModsecRule(
              getVariableString(clauses.get(i)),
              getOperatorString(clauses.get(i)),
              modsecActions.getChainedRuleIntermediateActionsString()));
    }
    modsecRules.add(
        modsecRuleMappings.getModsecRule(
            getVariableString(clauses.get(size - 1)),
            getOperatorString(clauses.get(size - 1)),
            modsecActions.getChainedRuleFinalActionsString()));

    return String.join(NEW_LINE_DELIMITER, modsecRules);
  }

  private String getVariableString(Clause clause) {
    switch (clause.getClauseCase()) {
      case MATCH_EXPRESSION:
        return modsecRuleMappings.getVariableString(
            clause.getMatchExpression().getMatchCategory(),
            clause.getMatchExpression().getMatchKey());
      case KEY_VALUE_EXPRESSION:
        return modsecRuleMappings.getVariableString(
            clause.getKeyValueExpression().getMatchCategory(),
            clause.getKeyValueExpression().getTag(),
            clause.getKeyValueExpression().getMatchKey(),
            clause.getKeyValueExpression().getKeyMatchOperator());
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate variable for unknown clause type '%s'", clause.getClauseCase()));
    }
  }

  private String getOperatorString(Clause clause) {
    switch (clause.getClauseCase()) {
      case MATCH_EXPRESSION:
        return modsecRuleMappings.getOperatorString(
            clause.getMatchExpression().getMatchOperator(),
            clause.getMatchExpression().getMatchValue());
      case KEY_VALUE_EXPRESSION:
        return modsecRuleMappings.getOperatorString(
            clause.getKeyValueExpression().getValueMatchOperator(),
            clause.getKeyValueExpression().getMatchValue());
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate operator for unknown clause type '%s'", clause.getClauseCase()));
    }
  }

  private boolean hasResponseVariable(Clause clause) {
    switch (clause.getClauseCase()) {
      case MATCH_EXPRESSION:
        return clause
            .getMatchExpression()
            .getMatchCategory()
            .equals(MatchCategory.MATCH_CATEGORY_RESPONSE);
      case KEY_VALUE_EXPRESSION:
        return clause
            .getKeyValueExpression()
            .getMatchCategory()
            .equals(MatchCategory.MATCH_CATEGORY_RESPONSE);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate rule for unknown clause type '%s'", clause.getClauseCase()));
    }
  }
}
