package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.customsignature.config.service.modsec.directives.ModsecDirectivesManager;
import ai.traceable.customsignature.config.service.modsec.registry.ModsecActions;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleDetails;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.modsecurity.RuleEngine;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomSignatureModsecRulesManager implements ModsecRulesManager {
  private static final long MODSEC_ID_SEED = 10000000;
  private static final String RANDOM_RULE_ID = UUID.randomUUID().toString();
  private static final String NEW_LINES_DELIMITER = "\n\n";

  @VisibleForTesting static final boolean loadNativeLibrarySuccess = loadNativeRuleEngineLibrary();

  private static boolean loadNativeRuleEngineLibrary() {
    try {
      RuleEngine.loadNativeLibrary();
      return true;
    } catch (IOException e) {
      log.warn("Failed loading rule engine native library with error:", e);
      return false;
    }
  }

  private final ModsecRuleConversion modsecRuleConversion;
  private final String modsecConfigDirectives;

  @Inject
  public CustomSignatureModsecRulesManager(
      ModsecRuleConversion modsecRuleConversion,
      ModsecDirectivesManager modsecDirectivesManager,
      ModsecRuleVersion modsecRuleVersion) {
    this.modsecRuleConversion = modsecRuleConversion;
    modsecConfigDirectives = modsecDirectivesManager.getModsecHeader(modsecRuleVersion);
  }

  @Override
  public GetCustomSignatureModsecRulesResponse getModsecRules(
      List<CustomSignatureRule> customSignatureRules) {
    List<CustomSignatureRuleDetails> ruleDetailsList = new ArrayList<>();
    List<String> allowModsecRules = new ArrayList<>();
    List<String> violationModsecRules = new ArrayList<>();

    long modsecIdAssignment = MODSEC_ID_SEED;

    for (CustomSignatureRule rule : customSignatureRules) {
      String modsecRule;
      modsecIdAssignment++;
      try {
        modsecRule =
            getModsecRule(rule.getId(), rule.getName(), rule.getDefinition(), modsecIdAssignment);
      } catch (Exception ex) {
        log.warn(
            "Modsec rule could not be created for rule: {}, exception: {}", rule.getName(), ex);
        continue;
      }
      if (modsecRule == null || modsecRule.isBlank()) {
        continue;
      }
      if (rule.getEffect().getEventType() == EventType.EVENT_TYPE_ALLOW) {
        allowModsecRules.add(modsecRule);
      } else {
        violationModsecRules.add(modsecRule);
      }
      ruleDetailsList.add(
          CustomSignatureRuleDetails.newBuilder()
              .setId(rule.getId())
              .setName(rule.getName())
              .setEffect(rule.getEffect())
              .setDisabled(rule.getDisabled())
              .setInternal(rule.getInternal())
              .setBlockingExpiryDetails(rule.getBlockingExpiryDetails())
              .setRuleScope(rule.getRuleScope())
              .build());
    }

    if (ruleDetailsList.isEmpty()) {
      return GetCustomSignatureModsecRulesResponse.getDefaultInstance();
    }
    List<String> modsecRules =
        Stream.concat(allowModsecRules.stream(), violationModsecRules.stream())
            .collect(Collectors.toList());
    return GetCustomSignatureModsecRulesResponse.newBuilder()
        .setModsecRulesBlob(modsecConfigDirectives + String.join(NEW_LINES_DELIMITER, modsecRules))
        .addAllRules(ruleDetailsList)
        .build();
  }

  public Status validateModsecRule(String ruleName, RuleDefinition ruleDefinition) {
    if (loadNativeRuleEngineLibrary() == false) {
      log.warn("Skipping modsec validation.. Native libraries for rule engine not loaded!");
      return Status.OK;
    }
    String modsecRule;
    try {
      modsecRule = getModsecRule(RANDOM_RULE_ID, ruleName, ruleDefinition, MODSEC_ID_SEED);
    } catch (Exception ex) {
      return Status.INTERNAL
          .withCause(ex)
          .withDescription(
              String.format("Modsec rule could not be created for rule: [%s]", ruleName));
    }
    return validateModsecRule(modsecRule, ruleName);
  }

  @VisibleForTesting
  Status validateModsecRule(String modsecRule, String ruleName) {
    try {
      RuleEngine ruleEngine = RuleEngine.create(modsecRule);
      RuleEngine.destroy(ruleEngine);
      return Status.OK;
    } catch (Exception ex) {
      return Status.INTERNAL
          .withCause(ex)
          .withDescription(
              String.format(
                  "Invalid modsec rule: [%s] for rule:[%s]. Rule Engine could not be created.",
                  modsecRule, ruleName));
    }
  }

  private String getModsecRule(
      String ruleId, String ruleName, RuleDefinition ruleDefinition, long modsecIdAssignment)
      throws Exception {
    ClauseGroup clauseGroup = ruleDefinition.getClauseGroup();
    if (clauseGroup.getClauseOperator() != ClauseOperator.CLAUSE_OPERATOR_AND) {
      log.error(
          "Clause Operator {} is not supported for Rule {} (only AND clauses are supported)",
          clauseGroup.getClauseOperator(),
          ruleId);
      return null;
    }
    if (clauseGroup.getClausesList().isEmpty()) {
      log.warn("Clauses List is empty. So no modsec conversion possible for Rule {}", ruleId);
      return null;
    }
    ModsecActions modsecActions = new ModsecActions(modsecIdAssignment, ruleId, ruleName);
    return modsecRuleConversion.getModsecRuleForANDClauses(
        clauseGroup.getClausesList(), modsecActions);
  }
}
