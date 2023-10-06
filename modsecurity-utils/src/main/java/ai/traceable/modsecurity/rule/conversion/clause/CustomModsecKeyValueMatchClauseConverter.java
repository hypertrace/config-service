package ai.traceable.modsecurity.rule.conversion.clause;

import static ai.traceable.modsecurity.rule.conversion.clause.ModsecOperatorConverter.COUNT_EQUALS_ZERO_OPERATOR_EXPRESSION;

import ai.traceable.modsecurity.rule.api.v1.CustomModsecKeyValueMatchClause;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecMatchExpression;
import ai.traceable.modsecurity.rule.secrule.ModsecSecRule;
import ai.traceable.modsecurity.rule.secrule.operator.ModsecOperatorExpression;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariable;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariableMetadata;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariableMetadataKey;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariableMetadataOperator;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;

public class CustomModsecKeyValueMatchClauseConverter {

  private final ModsecVariableConverter variableConverter;
  private final ModsecOperatorConverter operatorConverter;

  @Inject
  public CustomModsecKeyValueMatchClauseConverter(
      ModsecVariableConverter variableConverter, ModsecOperatorConverter operatorConverter) {
    this.variableConverter = variableConverter;
    this.operatorConverter = operatorConverter;
  }

  public List<ModsecSecRule> getSecRules(CustomModsecKeyValueMatchClause clause) {
    if (clause.getKeyMatchExpression().equals(CustomModsecMatchExpression.getDefaultInstance())
        && clause
            .getValueMatchExpression()
            .equals(CustomModsecMatchExpression.getDefaultInstance())) {
      throw new IllegalArgumentException(
          "CustomModsecKeyValueMatchClause should have at least one match expression");
    }

    ModsecVariableMetadata modsecVariableMetadata = getModsecVariableMetadata(clause);

    if (clause.getKeyMatchExpression().equals(CustomModsecMatchExpression.getDefaultInstance())) {
      return getSecRules(modsecVariableMetadata, clause.getValueMatchExpression());
    } else if (clause
        .getValueMatchExpression()
        .equals(CustomModsecMatchExpression.getDefaultInstance())) {
      return getSecRules(modsecVariableMetadata, clause.getKeyMatchExpression());
    }

    ModsecOperatorExpression valueOperatorExpression =
        operatorConverter.getValueOperatorExpression(clause.getValueMatchExpression());
    return getSecRuleWithSimpleOperator(
            modsecVariableMetadata, clause.getKeyMatchExpression(), valueOperatorExpression)
        .map(Collections::singletonList)
        .or(
            () ->
                getSecRuleWithSimpleNotOperator(
                        modsecVariableMetadata,
                        clause.getKeyMatchExpression(),
                        valueOperatorExpression)
                    .map(Collections::singletonList))
        .or(
            () ->
                Optional.of(
                    getChainedSecRules(
                        modsecVariableMetadata,
                        clause.getKeyMatchExpression(),
                        valueOperatorExpression)))
        .orElse(Collections.emptyList());
  }

  private ModsecVariableMetadata getModsecVariableMetadata(CustomModsecKeyValueMatchClause clause) {
    if (clause.getValueMatchExpression().equals(CustomModsecMatchExpression.getDefaultInstance())) {
      switch (clause.getMatchMetadataCase()) {
        case REQUEST_METADATA:
          return variableConverter.getModsecKeyVariableMetadata(clause.getRequestMetadata());
        case RESPONSE_METADATA:
          return variableConverter.getModsecKeyVariableMetadata(clause.getResponseMetadata());
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported MatchMetadataCase: %s", clause.getMatchMetadataCase()));
      }
    } else {
      switch (clause.getMatchMetadataCase()) {
        case REQUEST_METADATA:
          return variableConverter.getModsecValueVariableMetadata(clause.getRequestMetadata());
        case RESPONSE_METADATA:
          return variableConverter.getModsecValueVariableMetadata(clause.getResponseMetadata());
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported MatchMetadataCase: %s", clause.getMatchMetadataCase()));
      }
    }
  }

  private List<ModsecSecRule> getSecRules(
      ModsecVariableMetadata modsecVariableMetadata, CustomModsecMatchExpression matchExpression) {
    return getSecRuleWithSimpleOperator(modsecVariableMetadata, matchExpression)
        .map(Collections::singletonList)
        .or(
            () ->
                getSecRuleWithSimpleNotOperator(modsecVariableMetadata, matchExpression)
                    .map(Collections::singletonList))
        .or(() -> Optional.of(getChainedSecRules(modsecVariableMetadata, matchExpression)))
        .orElse(Collections.emptyList());
  }

  // returns `SecRule METADATA "@operator value" actions`
  private Optional<ModsecSecRule> getSecRuleWithSimpleOperator(
      ModsecVariableMetadata modsecVariableMetadata, CustomModsecMatchExpression matchExpression) {
    return operatorConverter
        .getOnlyPositiveValueOperatorExpression(matchExpression)
        .map(
            exp ->
                ModsecSecRule.builder()
                    .variables(
                        Collections.singletonList(new ModsecVariable(modsecVariableMetadata)))
                    .operatorExpression(exp)
                    .build());
  }

  // returns `SecRule &METADATA:value "@eq 0" actions`
  private Optional<ModsecSecRule> getSecRuleWithSimpleNotOperator(
      ModsecVariableMetadata modsecVariableMetadata, CustomModsecMatchExpression matchExpression) {
    return operatorConverter
        .getOppositePositiveVariableMetadataOperator(matchExpression)
        .map(
            operator ->
                new ModsecVariable(
                    modsecVariableMetadata,
                    ModsecVariableMetadataOperator.COUNT,
                    new ModsecVariableMetadataKey(operator, matchExpression.getMatchValue())))
        .map(
            variable ->
                ModsecSecRule.builder()
                    .variables(Collections.singletonList(variable))
                    .operatorExpression(COUNT_EQUALS_ZERO_OPERATOR_EXPRESSION)
                    .build());
  }

  // returns
  // `SecRule METADATA "@+veOperator value" actions-with-chain
  //  SecRule &MATCHED_VARS "eq 0" actions`
  private List<ModsecSecRule> getChainedSecRules(
      ModsecVariableMetadata modsecVariableMetadata, CustomModsecMatchExpression matchExpression) {
    ModsecOperatorExpression positiveOperator =
        operatorConverter.getOppositePositiveOperatorExpression(matchExpression);
    ModsecSecRule primaryRule =
        ModsecSecRule.builder()
            .variables(Collections.singletonList(new ModsecVariable(modsecVariableMetadata)))
            .operatorExpression(positiveOperator)
            .build();
    ModsecSecRule chainedRule =
        ModsecSecRule.builder()
            .variables(
                Collections.singletonList(
                    new ModsecVariable(
                        ModsecVariableMetadata.MATCHED_VARS, ModsecVariableMetadataOperator.COUNT)))
            .operatorExpression(COUNT_EQUALS_ZERO_OPERATOR_EXPRESSION)
            .build();
    return List.of(primaryRule, chainedRule);
  }

  // returns `SecRule METADATA:key "@operator value" actions`
  private Optional<ModsecSecRule> getSecRuleWithSimpleOperator(
      ModsecVariableMetadata modsecVariableMetadata,
      CustomModsecMatchExpression keyMatchExpression,
      ModsecOperatorExpression valueOperatorExpression) {
    return operatorConverter
        .getVariableMetadataOperator(keyMatchExpression)
        .map(
            op ->
                ModsecSecRule.builder()
                    .variables(
                        Collections.singletonList(
                            new ModsecVariable(
                                modsecVariableMetadata,
                                new ModsecVariableMetadataKey(
                                    op, keyMatchExpression.getMatchValue()))))
                    .operatorExpression(valueOperatorExpression)
                    .build());
  }

  // returns `SecRule METADATA:key "@operator value" actions`
  private Optional<ModsecSecRule> getSecRuleWithSimpleNotOperator(
      ModsecVariableMetadata modsecVariableMetadata,
      CustomModsecMatchExpression keyMatchExpression,
      ModsecOperatorExpression valueOperatorExpression) {
    return operatorConverter
        .getOppositePositiveVariableMetadataOperator(keyMatchExpression)
        .map(
            op ->
                ModsecSecRule.builder()
                    .variables(
                        List.of(
                            new ModsecVariable(modsecVariableMetadata),
                            new ModsecVariable(
                                modsecVariableMetadata,
                                ModsecVariableMetadataOperator.NOT,
                                new ModsecVariableMetadataKey(
                                    op, keyMatchExpression.getMatchValue()))))
                    .operatorExpression(valueOperatorExpression)
                    .build());
  }

  // returns
  // `SecRule METADATA "@operator value" actions-with-chain
  //  SecRule MATCHED_VARS_NAMES "@operator key" actions`
  private List<ModsecSecRule> getChainedSecRules(
      ModsecVariableMetadata modsecVariableMetadata,
      CustomModsecMatchExpression keyMatchExpression,
      ModsecOperatorExpression valueOperatorExpression) {
    ModsecSecRule primaryRule =
        ModsecSecRule.builder()
            .variables(Collections.singletonList(new ModsecVariable(modsecVariableMetadata)))
            .operatorExpression(valueOperatorExpression)
            .build();
    ModsecSecRule chainedRule =
        ModsecSecRule.builder()
            .variables(
                Collections.singletonList(
                    new ModsecVariable(ModsecVariableMetadata.MATCHED_VARS_NAMES)))
            .operatorExpression(operatorConverter.getValueOperatorExpression(keyMatchExpression))
            .build();
    return List.of(primaryRule, chainedRule);
  }
}
