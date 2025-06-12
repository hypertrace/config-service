package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.customsignature.config.service.modsec.directives.ModsecDirectivesManager;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
      CustomModsecRuleVersion customModsecRuleVersion,
      boolean includeAllPartialModsecRules) {
    List<CustomSignatureInlineRule> inlineRuleList = new ArrayList<>();
    List<String> allowModsecRules = new ArrayList<>();
    List<String> violationModsecRules = new ArrayList<>();

    long modsecIdAssignment = MODSEC_ID_SEED;

    for (CustomSignatureRule rule : customSignatureRules) {
      if (!includeAllPartialModsecRules
          && !ModsecRulesSupportChecker.isInlineRuleMappingSupported(
              rule.getDefinition().getClauseGroup())) {
        log.debug(
            "Inline rule mapping is not supported for rule - rule ID: {} tenant ID: {}",
            rule,
            requestContext.getTenantId().orElse("Unknown"));
        continue;
      }
      List<Clause> modsecConvertibleClauses =
          getModsecConvertibleClauses(rule.getDefinition().getClauseGroup());
      if (modsecConvertibleClauses.isEmpty()) {
        inlineRuleList.add(CustomSignatureInlineRule.newBuilder().setRule(rule).build());
        continue;
      }

      String modsecRule;
      modsecIdAssignment++;
      try {
        modsecRule =
            customModsecRuleConverter.getValidatedModsecRule(
                modsecIdAssignment, rule.getId(), rule.getName(), modsecConvertibleClauses);
      } catch (Exception ex) {
        log.warn(
            "For tenant id - {} Modsec rule could not be created for rule: {}, exception: {}",
            requestContext.getTenantId().orElse("Unknown"),
            rule.getName(),
            ex);
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
      inlineRuleList.add(CustomSignatureInlineRule.newBuilder().setRule(rule).build());
    }

    if (inlineRuleList.isEmpty()) {
      return GetCustomSignatureModsecRulesResponse.getDefaultInstance();
    }
    if (allowModsecRules.isEmpty() && violationModsecRules.isEmpty()) {
      return GetCustomSignatureModsecRulesResponse.newBuilder()
          .addAllInlineRules(inlineRuleList)
          .build();
    }
    String modsecRulesBlob =
        getModsecDirective(customModsecRuleVersion)
            + Stream.concat(allowModsecRules.stream(), violationModsecRules.stream())
                .collect(Collectors.joining(NEW_LINES_DELIMITER));
    return GetCustomSignatureModsecRulesResponse.newBuilder()
        .setModsecRulesBlob(modsecRulesBlob)
        .addAllInlineRules(inlineRuleList)
        .build();
  }

  @Override
  public Status validateModsecRule(String ruleName, RuleDefinition ruleDefinition) {
    try {
      List<Clause> modsecConvertibleClauses =
          getModsecConvertibleClauses(ruleDefinition.getClauseGroup());
      customModsecRuleConverter.getValidatedModsecRule(
          MODSEC_ID_SEED, RANDOM_RULE_ID, ruleName, modsecConvertibleClauses);
      return Status.OK;
    } catch (Exception ex) {
      return Status.INTERNAL
          .withCause(ex)
          .withDescription(
              String.format("Modsec rule could not be created for rule: [%s]", ruleName));
    }
  }

  @Override
  public boolean containsModsecConvertibleClauses(ClauseGroup clauseGroup) {
    return !getModsecConvertibleClauses(clauseGroup).isEmpty();
  }

  private List<Clause> getModsecConvertibleClauses(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream()
        .filter(
            clause ->
                clause.hasCustomSecRule()
                    || clause.hasMatchExpression()
                    || clause.hasKeyValueExpression())
        .collect(Collectors.toList());
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
