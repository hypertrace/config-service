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
import lombok.Setter;

public class ModsecSecRuleGroup {
  public static final String CHAIN = "chain";
  public static final String MSG = "msg:";
  public static final String LOGDATA = "logdata:";
  private final ModsecActions modsecActions;
  private final List<SecRuleContainer> rules;

  private ModsecSecRuleGroup(ModsecActions modsecActions, List<SecRuleContainer> rules) {
    this.modsecActions = modsecActions;
    this.rules = rules;
  }

  public static ModsecRuleGroupBuilder builder() {
    return new ModsecRuleGroupBuilder();
  }

  private List<String> buildModsecRuleString() {
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
    return modsecRules;
  }

  public String getJNIValidatedModsecRuleString() {
    List<String> modsecRules = buildModsecRuleString();
    validate(modsecRules);
    String modsecRuleString = String.join(NEW_LINE_DELIMITER, modsecRules);
    validateModsec(modsecRuleString);
    return modsecRuleString;
  }

  public String getValidatedModsecRuleString() {
    List<String> modsecRules = buildModsecRuleString();
    validate(modsecRules);
    String modsecRuleString = String.join(NEW_LINE_DELIMITER, modsecRules);
    validate(modsecRuleString);
    return modsecRuleString;
  }

  private static void validate(String modsecRuleString) {
    validateModsec(modsecRuleString);
    validateCoraza(modsecRuleString);
  }

  private static void validateModsec(String modsecRuleString) {
    // Validate using ModSec
    Status modsecStatus = ModsecRuleEngineUtils.modsecValidate(modsecRuleString);
    if (Status.INVALID_ARGUMENT.getCode().equals(modsecStatus.getCode())) {
      throw modsecStatus.asRuntimeException();
    }
  }

  private static void validateCoraza(String modsecRuleString) {
    // Validate using Coraza
    Status corazaStatus = ModsecRuleEngineUtils.corazaValidate(modsecRuleString);
    if (Status.INVALID_ARGUMENT.getCode().equals(corazaStatus.getCode())) {
      throw corazaStatus.asRuntimeException();
    }
  }

  private static void validate(List<String> modsecRules) {
    for (String modsecRule : modsecRules) {
      String[] modsecSubRules = modsecRule.split(CHAIN);
      for (String modsecSubRule : modsecSubRules) {
        if (!modsecSubRule.contains(MSG) && modsecSubRule.contains(LOGDATA)) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Logdata and msg both should be present or absent for all the chained modsec sub rule for modsec rule %s",
                      modsecRule))
              .asRuntimeException();
        }
      }
    }
  }

  @Setter
  public static class ModsecRuleGroupBuilder {
    private final List<SecRuleContainer> secRules = new ArrayList<>();
    private long id;
    private String msg;
    private String ruleUuid;
    private String logData;
    private boolean responsePhase = false;

    private ModsecRuleGroupBuilder() {}

    public void addSecRule(ModsecSecRule secRule) {
      responsePhase |=
          secRule.getVariables().stream()
              .map(ModsecVariable::getMetadata)
              .anyMatch(ModsecVariableMetadata::needsResponsePhase);
      secRules.add(secRule);
    }

    public void addSecRules(List<ModsecSecRule> secRules) {
      responsePhase |=
          secRules.stream()
              .map(ModsecSecRule::getVariables)
              .flatMap(List::stream)
              .map(ModsecVariable::getMetadata)
              .anyMatch(ModsecVariableMetadata::needsResponsePhase);
      this.secRules.addAll(secRules);
    }

    public void addCustomSecRule(CustomSecRule customSecRule) {
      secRules.add(customSecRule);
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
              responsePhase);

      return new ModsecSecRuleGroup(modsecActions, secRules);
    }

    private boolean isNullOrBlank(String str) {
      return str == null || str.isBlank();
    }
  }
}
