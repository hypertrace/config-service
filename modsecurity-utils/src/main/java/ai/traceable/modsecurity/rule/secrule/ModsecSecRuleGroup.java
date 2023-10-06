package ai.traceable.modsecurity.rule.secrule;

import static ai.traceable.modsecurity.rule.secrule.ModsecRuleConstants.NEW_LINE_DELIMITER;

import ai.traceable.modsecurity.rule.secrule.actions.ModsecActions;
import ai.traceable.modsecurity.rule.secrule.actions.ModsecActionsType;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariable;
import ai.traceable.modsecurity.rule.secrule.variables.ModsecVariableMetadata;
import ai.traceable.modsecurity.utils.ModsecRuleEngineUtils;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ModsecSecRuleGroup {
  private final List<ModsecSecRule> secRules = new ArrayList<>();
  private final ModsecActions modsecActions;

  public ModsecSecRuleGroup(long id, String ruleUuid, String msg, Optional<String> logData) {
    this.modsecActions = new ModsecActions(id, ruleUuid, msg, logData);
  }

  public void addSecRule(ModsecSecRule secRule) {
    this.secRules.add(secRule);
  }

  public void addSecRules(List<ModsecSecRule> secRules) {
    this.secRules.addAll(secRules);
  }

  public String getValidatedModsecRuleString() {
    if (secRules.isEmpty()) {
      throw new IllegalArgumentException("At least 1 SecRule needs to be provided");
    }
    boolean responsePhase =
        secRules.stream()
            .map(ModsecSecRule::getVariables)
            .flatMap(List::stream)
            .map(ModsecVariable::getMetadata)
            .anyMatch(ModsecVariableMetadata::needsResponsePhase);

    int size = secRules.size();
    List<String> modsecRules = new ArrayList<>();

    if (size == 1) {
      modsecRules.add(
          secRules
              .get(0)
              .getSecRuleString(
                  modsecActions.getActionsString(ModsecActionsType.SINGULAR, responsePhase)));
    } else {
      modsecRules.add(
          secRules
              .get(0)
              .getSecRuleString(
                  modsecActions.getActionsString(
                      ModsecActionsType.CHAINED_PRIMARY, responsePhase)));
      for (int i = 1; i < size - 1; i++) {
        modsecRules.add(
            secRules
                .get(i)
                .getSecRuleString(
                    modsecActions.getActionsString(
                        ModsecActionsType.CHAINED_INTERMEDIATE, responsePhase)));
      }
      modsecRules.add(
          secRules
              .get(size - 1)
              .getSecRuleString(
                  modsecActions.getActionsString(ModsecActionsType.CHAINED_FINAL, responsePhase)));
    }

    String modsecRuleString = String.join(NEW_LINE_DELIMITER, modsecRules);
    validate(modsecRuleString);
    return modsecRuleString;
  }

  private static void validate(String modsecRuleString) {
    Status status = ModsecRuleEngineUtils.validate(modsecRuleString);
    if (Status.INVALID_ARGUMENT.getCode().equals(status.getCode())) {
      throw status.asRuntimeException();
    }
  }
}
