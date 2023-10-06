package ai.traceable.modsecurity.rule.conversion.clause;

import ai.traceable.modsecurity.rule.api.v1.CustomModsecValueMatchClause;
import ai.traceable.modsecurity.rule.secrule.ModsecSecRule;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariable;
import java.util.List;
import javax.inject.Inject;

public class CustomModsecValueMatchClauseConverter {

  private final ModsecVariableConverter variableConverter;
  private final ModsecOperatorConverter operatorConverter;

  @Inject
  public CustomModsecValueMatchClauseConverter(
      ModsecVariableConverter variableConverter, ModsecOperatorConverter operatorConverter) {
    this.variableConverter = variableConverter;
    this.operatorConverter = operatorConverter;
  }

  public ModsecSecRule getSecRule(CustomModsecValueMatchClause clause) {
    return ModsecSecRule.builder()
        .variables(getModsecVariables(clause))
        .operatorExpression(
            operatorConverter.getValueOperatorExpression(clause.getValueMatchExpression()))
        .build();
  }

  private List<ModsecVariable> getModsecVariables(CustomModsecValueMatchClause clause) {
    switch (clause.getMatchMetadataCase()) {
      case REQUEST_VALUE_METADATA:
        return variableConverter.getModsecVariables(clause.getRequestValueMetadata());
      case RESPONSE_VALUE_METADATA:
        return variableConverter.getModsecVariables(clause.getResponseValueMetadata());
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported MatchMetadataCase: %s", clause.getMatchMetadataCase()));
    }
  }
}
