package ai.traceable.modsecurity.rule.secrule;

import ai.traceable.modsecurity.rule.api.v1.CustomSecRuleClause;
import ai.traceable.modsecurity.rule.secrule.actions.ModsecActions;
import ai.traceable.modsecurity.rule.secrule.actions.ModsecActionsType;

public class CustomSecRule implements SecRuleContainer {
  private final String inputSecRule;

  public CustomSecRule(CustomSecRuleClause customSecRuleClause) {
    this.inputSecRule = customSecRuleClause.getInputSecRule();
  }

  @Override
  public String getSecRuleString(ModsecActions modsecActions, ModsecActionsType actionsType) {
    return modsecActions.modifyActionsStringInSecRule(inputSecRule, actionsType);
  }
}
