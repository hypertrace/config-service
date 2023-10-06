package ai.traceable.modsecurity.rule.secrule;

import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.PIPE;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SEC_RULE;
import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.SPACE_DELIMITER;

import ai.traceable.modsecurity.rule.secrule.operator.ModsecOperatorExpression;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariable;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
public class ModsecSecRule {
  private final List<ModsecVariable> variables;
  private final ModsecOperatorExpression operatorExpression;

  public ModsecSecRule(
      List<ModsecVariable> variables, ModsecOperatorExpression operatorExpression) {
    this.variables = variables;
    this.operatorExpression = operatorExpression;
  }

  List<ModsecVariable> getVariables() {
    return variables;
  }

  public String getSecRuleString(String modsecActions) {
    if (variables.isEmpty()) {
      return "";
    }
    String variableString =
        String.join(
            PIPE,
            variables.stream()
                .map(ModsecVariable::toString)
                .collect(Collectors.toUnmodifiableList()));
    return String.join(
        SPACE_DELIMITER, SEC_RULE, variableString, operatorExpression.toString(), modsecActions);
  }
}
