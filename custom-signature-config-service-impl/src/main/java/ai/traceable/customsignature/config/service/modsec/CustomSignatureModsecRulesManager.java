package ai.traceable.customsignature.config.service.modsec;

import static ai.traceable.customsignature.config.service.v1.Clause.ClauseCase.SCOPE_EXPRESSION;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
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
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import ai.traceable.customsignature.config.service.v1.ScopeExpression;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
  private static final String EMPTY_BLOB = "";
  private static final String MODSEC_MATCH_MESSAGE =
      "URL Regex and key-value conditions corresponding to custom signature rule - %s";

  private static final EnumSet<Clause.ClauseCase> NON_SOURCE_OR_TARGET_BASED_CLAUSES =
      EnumSet.of(
          Clause.ClauseCase.MATCH_EXPRESSION,
          Clause.ClauseCase.KEY_VALUE_EXPRESSION,
          Clause.ClauseCase.ATTRIBUTE_KEY_VALUE_EXPRESSION,
          Clause.ClauseCase.CUSTOM_SEC_RULE);

  private final CustomModsecRuleConverter customModsecRuleConverter;
  private final ModsecDirectivesManager modsecDirectivesManager;
  private final ModsecRulesRegistry modsecRulesRegistry;
  private final ModsecBlobValidator modsecBlobValidator;
  private final CachedServiceMappingProvider cachedServiceMappingProvider;

  @Inject
  public CustomSignatureModsecRulesManager(
      CustomModsecRuleConverter customModsecRuleConverter,
      ModsecDirectivesManager modsecDirectivesManager,
      ModsecRulesRegistry modsecRulesRegistry,
      ModsecBlobValidator modsecBlobValidator,
      CachedServiceMappingProvider cachedServiceMappingProvider) {
    this.customModsecRuleConverter = customModsecRuleConverter;
    this.modsecDirectivesManager = modsecDirectivesManager;
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.modsecBlobValidator = modsecBlobValidator;
    this.cachedServiceMappingProvider = cachedServiceMappingProvider;
  }

  @Override
  public GetCustomSignatureModsecRulesResponse getModsecRules(
      RequestContext requestContext,
      List<CustomSignatureRule> customSignatureRules,
      CustomModsecRuleVersion customModsecRuleVersion,
      boolean includeAllPartialModsecRules,
      ModsecCrsRulesTarget modsecCrsRulesTarget,
      List<String> serviceNames) {
    /*
     * This filters out rules with source-based or target-based clauses in case the crs rules target
     * is of type TPA.
     * These rules are excluded because TPA doesn't have information about them.
     */
    List<CustomSignatureRule> filteredCustomSignatureRules =
        modsecCrsRulesTarget == ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TPA_DETECTION
            ? customSignatureRules.stream()
                .filter(
                    rule ->
                        rule.getDefinition().getClauseGroup().getClausesList().stream()
                            .map(Clause::getClauseCase)
                            .allMatch(NON_SOURCE_OR_TARGET_BASED_CLAUSES::contains))
                .collect(Collectors.toList())
            : customSignatureRules;

    // filtering on the basis of rule evaluation point
    filteredCustomSignatureRules =
        filteredCustomSignatureRules.stream()
            .filter(
                rule ->
                    rule.getEffect()
                        .getRuleEvaluationPointsList()
                        .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .collect(Collectors.toUnmodifiableList());

    List<CustomSignatureInlineRule> inlineRuleList = new ArrayList<>();
    List<String> allowModsecRules = new ArrayList<>();
    List<String> violationModsecRules = new ArrayList<>();

    /*
     * Numbering rules to generate unique ids for each modsec rule
     * Using the same seed to keep modsec rules blob the same if nothing else changes
     */
    long modsecIdAssignment = MODSEC_ID_SEED;

    // Map to hold service-specific ModSec blobs and rule IDs as pairs
    Map<String, List<ModsecBlobResult>> serviceToModsecBlobDataMap = new LinkedHashMap<>();
    // Ensuring that all services have at least an empty blob
    serviceNames.forEach(
        serviceName ->
            serviceToModsecBlobDataMap.computeIfAbsent(serviceName, k -> new ArrayList<>()));

    for (CustomSignatureRule customSignatureRule : filteredCustomSignatureRules) {
      if (!includeAllPartialModsecRules
          && !ModsecRulesSupportChecker.isInlineRuleMappingSupported(
              customSignatureRule.getDefinition().getClauseGroup())) {
        log.debug(
            "Inline rule mapping is not supported for rule - rule ID: {} tenant ID: {}",
            customSignatureRule,
            requestContext.getTenantId().orElse("Unknown"));
        continue;
      }

      String modsecBlob;
      try {
        // Extract the applicable services first
        List<String> applicableServices =
            servicesOnWhichRuleIsApplicable(requestContext, customSignatureRule, serviceNames);

        // Filter out clauses which can't be converted into modsec rule
        List<Clause> modsecConvertibleClauses =
            getModsecConvertibleClauses(customSignatureRule.getDefinition().getClauseGroup());

        if (modsecConvertibleClauses.isEmpty()) {
          if (containsNonModsecTrackableClauses(
              customSignatureRule.getDefinition().getClauseGroup())) {
            ModsecBlobResult emptyModsecBlobResult =
                new ModsecBlobResult(EMPTY_BLOB, customSignatureRule.getId());
            applicableServices.forEach(
                applicableService ->
                    serviceToModsecBlobDataMap.get(applicableService).add(emptyModsecBlobResult));
          }
          inlineRuleList.add(
              CustomSignatureInlineRule.newBuilder().setRule(customSignatureRule).build());
          continue;
        }

        ModsecBlobResult modsecBlobResult =
            getModsecBlobResult(
                customSignatureRule.getId(), modsecConvertibleClauses, modsecIdAssignment++);
        modsecBlob = modsecBlobResult.getModsecBlob();

        applicableServices.forEach(
            applicableService ->
                serviceToModsecBlobDataMap.get(applicableService).add(modsecBlobResult));
      } catch (Exception ex) {
        log.warn(
            "For tenant id - {} Modsec rule could not be created for rule: {}, exception: {}",
            requestContext.getTenantId().orElse("Unknown"),
            customSignatureRule.getName(),
            ex);
        continue;
      }

      if (modsecBlob == null || modsecBlob.isBlank()) {
        continue;
      }
      if (customSignatureRule.getEffect().getEventType() == EventType.EVENT_TYPE_ALLOW) {
        allowModsecRules.add(modsecBlob);
      } else {
        violationModsecRules.add(modsecBlob);
      }
      inlineRuleList.add(
          CustomSignatureInlineRule.newBuilder().setRule(customSignatureRule).build());
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
              String.format(
                  "Modsec rule could not be created for the custom signature rule having name: [%s]",
                  ruleName));
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
                    || (clause.hasScopeExpression() && clause.getScopeExpression().hasUrlScope()))
        .collect(Collectors.toList());
  }

  /**
   * Checks if the clause group contains IP-related or region-related clauses that should be tracked
   * in ModsecBlobData even though they can't be converted to ModSec format. These clauses are
   * handled outside ModSec (in blocking config).
   */
  private boolean containsNonModsecTrackableClauses(ClauseGroup clauseGroup) {
    return clauseGroup.getClausesList().stream().anyMatch(this::isNonModsecTrackableClause);
  }

  private boolean isNonModsecTrackableClause(Clause clause) {
    switch (clause.getClauseCase()) {
      case IP_ADDRESS_EXPRESSION:
      case IP_TYPE_EXPRESSION:
      case REGION_EXPRESSION:
        // These are handled in blocking config (outside ModSec) but should be tracked
        return true;
      default:
        return false;
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

  private List<String> servicesOnWhichRuleIsApplicable(
      RequestContext requestContext,
      CustomSignatureRule customSignatureRule,
      List<String> serviceNames) {
    List<ServiceDetail> serviceDetails = getServiceDetails(requestContext, customSignatureRule);

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

  private List<ServiceDetail> getServiceDetails(
      RequestContext requestContext, CustomSignatureRule customSignatureRule) {
    return customSignatureRule.getDefinition().getClauseGroup().getClausesList().stream()
        .filter(this::isClauseServiceScoped)
        .map(clause -> this.convertToServiceDetails(requestContext, clause.getScopeExpression()))
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private ModsecBlobData createCombinedBlob(
      RequestContext requestContext,
      String serviceName,
      List<ModsecBlobResult> modsecBlobRuleIdPairs) {
    String combinedBlob =
        modsecBlobRuleIdPairs.stream()
            .map(ModsecBlobResult::getModsecBlob)
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
                .map(ModsecBlobResult::getCustomSignatureRuleId)
                .filter(Predicate.not(String::isBlank))
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }

  private boolean isClauseServiceScoped(Clause clause) {
    return clause.getClauseCase().equals(SCOPE_EXPRESSION)
        && clause.getScopeExpression().hasEntityScope()
        && clause.getScopeExpression().getEntityScope().getEntityType()
            == ScopeExpression.EntityType.ENTITY_TYPE_SERVICE;
  }

  private Set<ServiceDetail> convertToServiceDetails(
      RequestContext requestContext, ScopeExpression scopeExpression) {
    return scopeExpression.getEntityScope().getEntityIdsList().stream()
        .distinct()
        .map(
            entityId ->
                cachedServiceMappingProvider.getServiceIdentifierEntity(requestContext, entityId))
        .flatMap(Optional::stream)
        .map(
            serviceIdentifierEntity ->
                new ServiceDetail(
                    serviceIdentifierEntity.getServiceName(), scopeExpression.getExclude()))
        .collect(Collectors.toUnmodifiableSet());
  }

  private ModsecBlobResult getModsecBlobResult(
      String ruleIdentifier, List<Clause> ANDClausesList, long modsecIdAssignment) {
    try {
      if (ANDClausesList.isEmpty()) {
        return new ModsecBlobResult(EMPTY_BLOB, ruleIdentifier);
      }
      return new ModsecBlobResult(
          customModsecRuleConverter.getValidatedModsecRule(
              modsecIdAssignment,
              ruleIdentifier,
              String.format(MODSEC_MATCH_MESSAGE, ruleIdentifier),
              ANDClausesList),
          ruleIdentifier);
    } catch (Exception e) {
      log.warn(
          "Cannot convert custom signature rule with id {} into modsec rule", ruleIdentifier, e);
      return new ModsecBlobResult(EMPTY_BLOB, "");
    }
  }

  @Value
  static class ServiceDetail {
    String serviceName;
    boolean exclude;
  }

  @Value
  static class ModsecBlobResult {
    String modsecBlob;
    String customSignatureRuleId;
  }
}
