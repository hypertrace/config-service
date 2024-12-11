package ai.traceable.modsecurity.rule.conversion;

import ai.traceable.modsecurity.rule.api.v1.CustomModsecRule;
import ai.traceable.modsecurity.rule.api.v1.CustomModsecRuleClause;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecKeyValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.conversion.clause.CustomModsecValueMatchClauseConverter;
import ai.traceable.modsecurity.rule.secrule.CustomSecRule;
import ai.traceable.modsecurity.rule.secrule.ModsecSecRuleGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class ModsecRuleConverterImpl implements ModsecRuleConverter {

  private final CustomModsecValueMatchClauseConverter valueMatchClauseConverter;
  private final CustomModsecKeyValueMatchClauseConverter keyValueMatchClauseConverter;

  @Inject
  public ModsecRuleConverterImpl(
      CustomModsecValueMatchClauseConverter valueMatchClauseConverter,
      CustomModsecKeyValueMatchClauseConverter keyValueMatchClauseConverter) {
    this.valueMatchClauseConverter = valueMatchClauseConverter;
    this.keyValueMatchClauseConverter = keyValueMatchClauseConverter;
  }

  @Override
  public String getJNIValidatedModsecRule(CustomModsecRule customModsecRule) throws Exception {
    ModsecSecRuleGroup modsecRule = getModsecRule(customModsecRule);
    return modsecRule.getJNIValidatedModsecRuleString();
  }

  @Override
  public String getValidatedModsecRule(CustomModsecRule customModsecRule) throws Exception {
    ModsecSecRuleGroup modsecRule = getModsecRule(customModsecRule);
    return modsecRule.getValidatedModsecRuleString();
  }

  private ModsecSecRuleGroup getModsecRule(CustomModsecRule customModsecRule) throws Exception {
    if (customModsecRule.getRuleId() <= 0 || customModsecRule.getRuleUuid().isBlank()) {
      throw new IllegalArgumentException(
          "Custom Modsec Rule should have a valid rule ID and a valid rule UUID");
    }
    ModsecSecRuleGroup.ModsecRuleGroupBuilder secRuleGroupBuilder = ModsecSecRuleGroup.builder();
    secRuleGroupBuilder.setId(customModsecRule.getRuleId());
    secRuleGroupBuilder.setMsg(customModsecRule.getRuleMsg());
    secRuleGroupBuilder.setRuleUuid(customModsecRule.getRuleUuid());
    secRuleGroupBuilder.setLogData(customModsecRule.getLogMessage());
    for (CustomModsecRuleClause clause : customModsecRule.getAndClausesList()) {
      switch (clause.getClauseCase()) {
        case VALUE_MATCH_CLAUSE:
          secRuleGroupBuilder.addSecRule(
              valueMatchClauseConverter.getSecRule(clause.getValueMatchClause()));
          break;
        case KEY_VALUE_MATCH_CLAUSE:
          secRuleGroupBuilder.addSecRules(
              keyValueMatchClauseConverter.getSecRules(clause.getKeyValueMatchClause()));
          break;
        case CUSTOM_SEC_RULE_CLAUSE:
          secRuleGroupBuilder.addCustomSecRule(new CustomSecRule(clause.getCustomSecRuleClause()));
          break;

        default:
          throw new IllegalArgumentException("Invalid Clause case: " + clause.getClauseCase());
      }
    }
    return secRuleGroupBuilder.build();
  }

  @Override
  public String getModsecRulesBlob(List<CustomModsecRule> customModsecRules) throws Exception {
    List<String> modsecRules = new ArrayList<>();
    for (CustomModsecRule rule : customModsecRules) {
      modsecRules.add(getValidatedModsecRule(rule));
    }
    return modsecRules.stream()
        .filter(Predicate.not(String::isBlank))
        .sorted()
        .collect(Collectors.joining(NEW_LINES_DELIMITER));
  }
}
