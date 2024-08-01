package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.customsignature.config.service.modsec.directives.ModsecDirectivesManager;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleDetails;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomSignatureModsecRulesManager implements ModsecRulesManager {
  private static final long MODSEC_ID_SEED = 10000000;
  private static final String RANDOM_RULE_ID = UUID.randomUUID().toString();
  private static final String NEW_LINES_DELIMITER = "\n\n";

  private final CustomModsecRuleConverter customModsecRuleConverter;
  private final ModsecDirectivesManager modsecDirectivesManager;

  @Inject
  public CustomSignatureModsecRulesManager(
      CustomModsecRuleConverter customModsecRuleConverter,
      ModsecDirectivesManager modsecDirectivesManager) {
    this.customModsecRuleConverter = customModsecRuleConverter;
    this.modsecDirectivesManager = modsecDirectivesManager;
  }

  @Override
  public GetCustomSignatureModsecRulesResponse getModsecRules(
      RequestContext requestContext,
      List<CustomSignatureRule> customSignatureRules,
      CustomModsecRuleVersion customModsecRuleVersion) {
    List<CustomSignatureRuleDetails> ruleDetailsList = new ArrayList<>();
    List<String> allowModsecRules = new ArrayList<>();
    List<String> violationModsecRules = new ArrayList<>();

    long modsecIdAssignment = MODSEC_ID_SEED;

    for (CustomSignatureRule rule : customSignatureRules) {
      // drop rule with attribute key value expression.
      if (rule.getDefinition().getClauseGroup().getClausesList().stream()
          .anyMatch(Clause::hasAttributeKeyValueExpression)) {
        log.debug(
            "Modsec conversion not supported for attribute criteria - rule ID: {} tenant ID: {}",
            rule,
            requestContext.getTenantId().orElse("Unknown"));
        continue;
      }
      String modsecRule;
      modsecIdAssignment++;
      try {
        modsecRule =
            customModsecRuleConverter.getValidatedModsecRule(
                modsecIdAssignment,
                rule.getId(),
                rule.getName(),
                rule.getDefinition().getClauseGroup().getClausesList());
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
        .setModsecRulesBlob(
            getModsecDirective(customModsecRuleVersion)
                + String.join(NEW_LINES_DELIMITER, modsecRules))
        .addAllRules(ruleDetailsList)
        .build();
  }

  public Status validateModsecRule(String ruleName, RuleDefinition ruleDefinition) {
    try {
      customModsecRuleConverter.getValidatedModsecRule(
          MODSEC_ID_SEED,
          RANDOM_RULE_ID,
          ruleName,
          ruleDefinition.getClauseGroup().getClausesList());
      return Status.OK;
    } catch (Exception ex) {
      return Status.INTERNAL
          .withCause(ex)
          .withDescription(
              String.format("Modsec rule could not be created for rule: [%s]", ruleName));
    }
  }

  private String getModsecDirective(CustomModsecRuleVersion customModsecRuleVersion) {
    switch (customModsecRuleVersion) {
      case CUSTOM_MODSEC_RULE_VERSION_V3:
        return modsecDirectivesManager.getModsecHeader(ModsecRuleVersion.MODSEC_RULE_VERSION_V3);
      case CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS:
        return modsecDirectivesManager.getModsecHeader(
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS);
      case CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3:
        return modsecDirectivesManager.getModsecHeader(
            ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3);
      case CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
        return modsecDirectivesManager.getModsecHeader(
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE);
      case CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3_DETECTION_ONLY_MODE:
        return modsecDirectivesManager.getModsecHeader(
            ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3_DETECTION_ONLY_MODE);
      default:
        return modsecDirectivesManager.getModsecHeader(ModsecRuleVersion.MODSEC_RULE_VERSION_V3);
    }
  }
}
