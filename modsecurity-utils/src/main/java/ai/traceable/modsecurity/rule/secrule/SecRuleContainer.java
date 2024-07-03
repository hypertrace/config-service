package ai.traceable.modsecurity.rule.secrule;

import ai.traceable.modsecurity.rule.secrule.actions.ModsecActions;
import ai.traceable.modsecurity.rule.secrule.actions.ModsecActionsType;

public interface SecRuleContainer {
  String getSecRuleString(ModsecActions modsecActions, ModsecActionsType actionsType);
}
