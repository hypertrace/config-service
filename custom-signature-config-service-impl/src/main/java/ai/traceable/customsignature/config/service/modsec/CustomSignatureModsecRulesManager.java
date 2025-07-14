package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.customsignature.config.service.modsec.directives.ModsecDirectivesManager;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.ModsecBlobData;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class CustomSignatureModsecRulesManager implements ModsecRulesManager {
  private static final long MODSEC_ID_SEED = 10000000;
  private static final String RANDOM_RULE_ID = UUID.randomUUID().toString();
  private static final String NEW_LINES_DELIMITER = "\n\n";

  private final CustomModsecRuleConverter customModsecRuleConverter;
  private final ModsecDirectivesManager modsecDirectivesManager;
  private final ModsecRulesRegistry modsecRulesRegistry;
  private final ModsecBlobValidator modsecBlobValidator;

  @Inject
  public CustomSignatureModsecRulesManager(
      CustomModsecRuleConverter customModsecRuleConverter,
      ModsecDirectivesManager modsecDirectivesManager,
      ModsecRulesRegistry modsecRulesRegistry,
      ModsecBlobValidator modsecBlobValidator) {
    this.customModsecRuleConverter = customModsecRuleConverter;
    this.modsecDirectivesManager = modsecDirectivesManager;
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.modsecBlobValidator = modsecBlobValidator;
  }

  @Override
  public GetCustomSignatureModsecRulesResponse getModsecRules(
      RequestContext requestContext,
      List<CustomSignatureRule> customSignatureRules,
      CustomModsecRuleVersion customModsecRuleVersion,
      boolean includeAllPartialModsecRules,
      List<String> serviceNames) {
    List<CustomSignatureInlineRule> inlineRuleList = new ArrayList<>();
    List<String> allowModsecRules = new ArrayList<>();
    List<String> violationModsecRules = new ArrayList<>();

    /*
     * Numbering rules to generate unique ids for each modsec rule
     * Using the same seed to keep modsec rules blob the same if nothing else changes
     */
    long modsecIdAssignment = MODSEC_ID_SEED;

    // Map to hold service-specific ModSec blobs and rule IDs as pairs
    Map<String, List<ModsecBlobRuleIdPair>> serviceToModsecBlobDataMap = new LinkedHashMap<>();
    // Ensuring that all services have at least an empty blob
    serviceNames.forEach(
        serviceName ->
            serviceToModsecBlobDataMap.computeIfAbsent(serviceName, k -> new ArrayList<>()));

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

      List<ServiceDetail> serviceDetails = new ArrayList<>();
      for (Clause modsecConvertibleClause : modsecConvertibleClauses) {
        serviceDetails.addAll(getServiceDetailsForModsecConvertibleClause(modsecConvertibleClause));
      }
      List<String> applicableServices =
          servicesOnWhichRuleIsApplicable(serviceDetails, serviceNames);
      applicableServices.forEach(
          applicableService ->
              serviceToModsecBlobDataMap
                  .get(applicableService)
                  .add(new ModsecBlobRuleIdPair(modsecRule, rule.getId())));
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

    // merging blobs across services with the same rules
    List<ModsecBlobData> modsecBlobDataList =
        serviceToModsecBlobDataMap.entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    entry -> createCombinedBlob(requestContext, entry.getKey(), entry.getValue()),
                    LinkedHashMap::new,
                    Collectors.mapping(Map.Entry::getKey, Collectors.toList())))
            .entrySet()
            .stream()
            .map(entry -> entry.getKey().toBuilder().addAllServiceNames(entry.getValue()).build())
            .collect(Collectors.toUnmodifiableList());

    return GetCustomSignatureModsecRulesResponse.newBuilder()
        .setModsecRulesBlob(modsecRulesBlob)
        .addAllInlineRules(inlineRuleList)
        .setModsecDirectivesBlob(
            modsecRulesRegistry.getModsecHeader(
                ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE))
        .addAllModsecBlobsData(modsecBlobDataList)
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
                    || clause.hasKeyValueExpression()
                    || clause.hasScopeExpression())
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

  private List<ServiceDetail> getServiceDetailsForModsecConvertibleClause(Clause clause) {
    switch (clause.getClauseCase()) {
      case SCOPE_EXPRESSION:
        return handleScopeExpression(clause.getScopeExpression());
      default:
        return List.of();
    }
  }

  private List<ServiceDetail> handleScopeExpression(ScopeExpression scopeExpression) {
    if (scopeExpression.hasEntityScope()
        && scopeExpression.getEntityScope().getEntityType()
            == ScopeExpression.EntityType.ENTITY_TYPE_SERVICE) {
      // TODO: This will be done as a part of the next PR
    } else if (scopeExpression.hasUrlScope()) {
      return List.of();
    }
    throw new UnsupportedOperationException(
        String.format("Cannot derive ServiceDetails for scope expression - %s", scopeExpression));
  }

  private List<String> servicesOnWhichRuleIsApplicable(
      List<ServiceDetail> serviceDetails, List<String> serviceNames) {
    // Case 1: if serviceDetails is empty, then all services in serviceNames are applicable
    if (serviceDetails.isEmpty()) {
      return serviceNames;
    }

    // collecting services that are to be included and those that are to be excluded
    List<String> servicesToBeIncluded =
        serviceDetails.stream()
            .filter(Predicate.not(ServiceDetail::isExclude))
            .map(ServiceDetail::getServiceName)
            .collect(Collectors.toUnmodifiableList());
    List<String> servicesToBeExcluded =
        serviceDetails.stream()
            .filter(ServiceDetail::isExclude)
            .map(ServiceDetail::getServiceName)
            .collect(Collectors.toUnmodifiableList());

    // Case 2: if the list servicesToBeIncluded is non-empty,
    // then an intersection of serviceNames and servicesToBeIncluded is applicable
    if (!servicesToBeIncluded.isEmpty()) {
      return serviceNames.stream()
          .filter(servicesToBeIncluded::contains)
          .collect(Collectors.toUnmodifiableList());
    }

    // Case 3: if the list servicesToBeIncluded is empty but servicesToBeExcluded is not,
    // then all those servicesNames are applicable that aren't present in servicesToBeExcluded
    return serviceNames.stream()
        .filter(Predicate.not(servicesToBeExcluded::contains))
        .collect(Collectors.toUnmodifiableList());
  }

  private ModsecBlobData createCombinedBlob(
      RequestContext requestContext,
      String serviceName,
      List<ModsecBlobRuleIdPair> modsecBlobRuleIdPairs) {
    String combinedBlob =
        modsecBlobRuleIdPairs.stream()
            .map(ModsecBlobRuleIdPair::getModsecBlob)
            .filter(Predicate.not(String::isBlank))
            .collect(Collectors.joining(NEW_LINES_DELIMITER));

    // validating the above combinedBlob
    return ModsecBlobData.newBuilder()
        .setModsecBlob(
            modsecBlobValidator.validate(requestContext, combinedBlob, serviceName)
                ? combinedBlob
                : "")
        .addAllCustomSignatureRuleIds(
            modsecBlobRuleIdPairs.stream()
                .map(ModsecBlobRuleIdPair::getCustomSignatureRuleId)
                .filter(Predicate.not(String::isBlank))
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  @Value
  static class ServiceDetail {
    String serviceName;
    boolean exclude;
  }

  @Value
  static class ModsecBlobRuleIdPair {
    String modsecBlob;
    String customSignatureRuleId;
  }
}
