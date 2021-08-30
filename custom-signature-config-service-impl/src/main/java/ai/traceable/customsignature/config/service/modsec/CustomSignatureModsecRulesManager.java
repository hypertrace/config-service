package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.customsignature.config.service.modsec.directives.ModsecDirectivesManager;
import ai.traceable.customsignature.config.service.modsec.registry.ModsecActions;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleDetails;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomSignatureModsecRulesManager implements ModsecRulesManager {

  private static final long MODSEC_ID_SEED = 10000000;
  private static final String NEW_LINES_DELIMITER = "\n\n";

  private final ModsecRuleConversion modsecRuleConversion;
  private final String modsecConfigDirectives;

  @Inject
  public CustomSignatureModsecRulesManager(
      ModsecRuleConversion modsecRuleConversion, ModsecDirectivesManager modsecDirectivesManager) {
    this.modsecRuleConversion = modsecRuleConversion;
    modsecConfigDirectives = modsecDirectivesManager.getModsecHeader();
  }

  @Override
  public GetCustomSignatureModsecRulesResponse getModsecRules(
      List<CustomSignatureRule> customSignatureRules) {
    List<CustomSignatureRuleDetails> ruleDetailsList = new ArrayList<>();
    List<String> modsecRules = new ArrayList<>();

    long modsecIdAssignment = MODSEC_ID_SEED + 1;

    for (CustomSignatureRule rule : customSignatureRules) {
      ClauseGroup clauseGroup = rule.getDefinition().getClauseGroup();
      if (clauseGroup.getClauseOperator() != ClauseOperator.CLAUSE_OPERATOR_AND) {
        log.error(
            "Clause Operator {} is not supported for Rule {} (only AND clauses are supported)",
            clauseGroup.getClauseOperator(),
            rule.getId());
        continue;
      }
      if (clauseGroup.getClausesList().isEmpty()) {
        log.warn(
            "Clauses List is empty. So no modsec conversion possible for Rule {}", rule.getId());
        continue;
      }
      try {
        ModsecActions modsecActions =
            new ModsecActions(modsecIdAssignment, rule.getId(), rule.getName());
        modsecRules.add(
            modsecRuleConversion.getModsecRuleForANDClauses(
                clauseGroup.getClausesList(), modsecActions));
        ruleDetailsList.add(
            CustomSignatureRuleDetails.newBuilder()
                .setId(rule.getId())
                .setName(rule.getName())
                .setEffect(rule.getEffect())
                .setDisabled(rule.getDisabled())
                .setInternal(rule.getInternal())
                .setBlockingExpiryDetails(rule.getBlockingExpiryDetails())
                .build());
        modsecIdAssignment++;
      } catch (Exception e) {
        log.error("Error in modsec conversion for Rule {} : {}", rule.getId(), e);
      }
    }

    if (ruleDetailsList.isEmpty()) {
      return GetCustomSignatureModsecRulesResponse.getDefaultInstance();
    }

    return GetCustomSignatureModsecRulesResponse.newBuilder()
        .setModsecRulesBlob(modsecConfigDirectives + String.join(NEW_LINES_DELIMITER, modsecRules))
        .addAllRules(ruleDetailsList)
        .build();
  }
}
