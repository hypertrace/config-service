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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Setter;

public class ModsecSecRuleGroup {
  private final ModsecActions modsecActions;
  private final List<SecRuleContainer> rules;

  private ModsecSecRuleGroup(ModsecActions modsecActions, List<SecRuleContainer> rules) {
    this.modsecActions = modsecActions;
    this.rules = rules;
  }

  public static ModsecRuleGroupBuilder builder() {
    return new ModsecRuleGroupBuilder();
  }

  public String getValidatedModsecRuleString() {
    if (rules.isEmpty()) {
      throw new IllegalArgumentException("At least 1 SecRule needs to be provided");
    }

    List<String> modsecRules = new ArrayList<>();
    if (rules.size() == 1) {
      modsecRules.add(rules.get(0).getSecRuleString(modsecActions, ModsecActionsType.SINGULAR));
    } else {
      modsecRules.add(
          rules.get(0).getSecRuleString(modsecActions, ModsecActionsType.CHAINED_PRIMARY));
      for (int i = 1; i < rules.size() - 1; i++) {
        modsecRules.add(
            rules.get(i).getSecRuleString(modsecActions, ModsecActionsType.CHAINED_INTERMEDIATE));
      }
      modsecRules.add(
          rules
              .get(rules.size() - 1)
              .getSecRuleString(modsecActions, ModsecActionsType.CHAINED_FINAL));
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

  @Setter
  public static class ModsecRuleGroupBuilder {
    private final List<ModsecSecRule> secRules = new ArrayList<>();
    private final List<CustomSecRule> customSecRules = new ArrayList<>();
    private long id;
    private String msg;
    private String ruleUuid;
    private String logData;

    private ModsecRuleGroupBuilder() {}

    public void addSecRule(ModsecSecRule secRule) {
      this.secRules.add(secRule);
    }

    public void addSecRules(List<ModsecSecRule> secRules) {
      this.secRules.addAll(secRules);
    }

    public void addCustomSecRule(CustomSecRule customSecRule) {
      this.customSecRules.add(customSecRule);
    }

    public ModsecSecRuleGroup build() {
      if (id <= 0) {
        throw new IllegalStateException("Invalid Rule ID:" + id);
      }
      if (isNullOrBlank(msg)) {
        throw new IllegalStateException("Invalid Rule Msg:" + msg);
      }
      if (isNullOrBlank(ruleUuid)) {
        throw new IllegalStateException("Invalid Rule UUID:" + ruleUuid);
      }
      ModsecActions modsecActions =
          new ModsecActions(
              id,
              ruleUuid,
              msg,
              Optional.ofNullable(logData).filter(Predicate.not(String::isBlank)),
              secRules.stream()
                  .map(ModsecSecRule::getVariables)
                  .flatMap(List::stream)
                  .map(ModsecVariable::getMetadata)
                  .anyMatch(ModsecVariableMetadata::needsResponsePhase));
      List<SecRuleContainer> rules =
          Stream.concat(customSecRules.stream(), secRules.stream())
              .collect(Collectors.toUnmodifiableList());
      return new ModsecSecRuleGroup(modsecActions, rules);
    }

    private boolean isNullOrBlank(String str) {
      return str == null || str.isBlank();
    }
  }
}
