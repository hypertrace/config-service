package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesResponse;
import ai.traceable.detection.exclusion.config.service.v1.ModsecBlobData;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecBlobConverter.ModsecBlobResult;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecClauseConverter.ModsecClauseResult;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecClauseConverter.ServiceDetail;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ExclusionModsecRulesManager {
  private static final long MODSEC_ID_SEED = 30000000;
  private static final String NEW_LINES_DELIMITER = "\n\n";

  private final ModsecRulesRegistry modsecRulesRegistry;
  private final ModsecClauseConverter modsecClauseConverter;
  private final ModsecBlobConverter modsecBlobConverter;
  private final ModsecBlobValidator modsecBlobValidator;

  @Inject
  public ExclusionModsecRulesManager(
      ModsecRulesRegistry modsecRulesRegistry,
      ModsecClauseConverter modsecClauseConverter,
      ModsecBlobConverter modsecBlobConverter,
      ModsecBlobValidator modsecBlobValidator) {
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.modsecClauseConverter = modsecClauseConverter;
    this.modsecBlobConverter = modsecBlobConverter;
    this.modsecBlobValidator = modsecBlobValidator;
  }

  public GetExclusionModsecRulesResponse getModsecRules(
      RequestContext requestContext,
      List<DetectionExclusionRule> exclusionRules,
      List<String> serviceNames) {
    // Numbering rules to generate unique ids for each modsec rule
    // Using the same seed to keep modsec rules blob same if nothing else changes
    AtomicLong modsecIdAssignment = new AtomicLong(MODSEC_ID_SEED);

    // Map to hold service-specific ModSec blobs and rule IDs as pairs
    Map<String, List<ModsecBlobResult>> serviceToModsecBlobDataMap = new LinkedHashMap<>();
    // Ensure that all services have at least an empty blob
    serviceNames.forEach(
        service -> serviceToModsecBlobDataMap.computeIfAbsent(service, k -> new ArrayList<>()));
    List<DetectionExclusionModsecRule> exclusionModsecRules = new ArrayList<>();

    // Convert exclusion rule to modsec blob
    exclusionRules.stream()
        .filter(
            detectionExclusionRule ->
                detectionExclusionRule
                    .getRuleInfo()
                    .getRuleEvaluationPointsList()
                    .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
        .forEach(
            exclusionRule -> {
              ModsecClauseResult modsecClauseResult =
                  modsecClauseConverter.convert(requestContext, exclusionRule);
              ModsecBlobResult modsecBlobResult =
                  modsecBlobConverter.convertToModsecRule(
                      exclusionRule.getId(), modsecClauseResult.getClauses(), modsecIdAssignment);
              List<String> servicesApplicable =
                  servicesOnWhichRuleIsApplicable(
                      modsecClauseResult.getServiceDetails(), serviceNames);

              // Store the modsecBlob and rule ID pair against all applicable services
              servicesApplicable.forEach(
                  service -> serviceToModsecBlobDataMap.get(service).add(modsecBlobResult));

              // Convert into DetectionExclusionModsecRule
              if (modsecBlobResult.getModsecBlob().isBlank()) {
                // If blob is empty, there would be no associated modsec rule id
                // There can be rules with request/response conditions, which would produce a blank
                // modsec blob, but we would still want to evaluate the rules
                exclusionModsecRules.add(
                    DetectionExclusionModsecRule.newBuilder().setRule(exclusionRule).build());
              } else {
                exclusionModsecRules.add(
                    DetectionExclusionModsecRule.newBuilder()
                        .setRule(exclusionRule)
                        .addAssociatedModsecRuleIds(modsecBlobResult.getRuleId())
                        .build());
              }
            });

    // Merge blobs across services with the same rules
    List<ModsecBlobData> modsecBlobDataList =
        serviceToModsecBlobDataMap.entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    entry -> createCombinedBlob(requestContext, entry.getKey(), entry.getValue()),
                    LinkedHashMap::new,
                    Collectors.mapping(Entry::getKey, Collectors.toList())))
            .entrySet()
            .stream()
            .map(entry -> entry.getKey().toBuilder().addAllServiceNames(entry.getValue()).build())
            .collect(Collectors.toUnmodifiableList());

    return GetExclusionModsecRulesResponse.newBuilder()
        .setModsecDirectivesBlob(
            modsecRulesRegistry.getModsecHeader(
                ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE))
        .addAllModsecBlobsData(modsecBlobDataList)
        .addAllModsecRules(exclusionModsecRules)
        .build();
  }

  private List<String> servicesOnWhichRuleIsApplicable(
      List<ServiceDetail> serviceDetails, List<String> serviceNames) {
    // Case 1: If serviceDetails is empty, all services in serviceNames are applicable
    if (serviceDetails.isEmpty()) {
      return serviceNames;
    }

    // Collect inclusion and exclusion services
    List<String> includeServices =
        serviceDetails.stream()
            .filter(Predicate.not(ServiceDetail::isExclude))
            .map(ServiceDetail::getServiceName)
            .collect(Collectors.toUnmodifiableList());

    List<String> excludeServices =
        serviceDetails.stream()
            .filter(ServiceDetail::isExclude)
            .map(ServiceDetail::getServiceName)
            .collect(Collectors.toUnmodifiableList());

    // Case 2: If there are inclusion services, return intersection of serviceNames and inclusion
    // services
    if (!includeServices.isEmpty()) {
      return serviceNames.stream()
          .filter(includeServices::contains)
          .collect(Collectors.toUnmodifiableList());
    }

    // Case 3: If inclusion services are empty but exclusion services are not,
    // return all serviceNames except those in the exclusion list
    return serviceNames.stream()
        .filter(Predicate.not(excludeServices::contains))
        .collect(Collectors.toUnmodifiableList());
  }

  private ModsecBlobData createCombinedBlob(
      RequestContext requestContext, String service, List<ModsecBlobResult> modsecBlobResults) {
    String combinedBlob =
        modsecBlobResults.stream()
            .map(ModsecBlobResult::getModsecBlob)
            .filter(Predicate.not(String::isBlank))
            .collect(Collectors.joining(NEW_LINES_DELIMITER));

    // Validate the combined blob; if invalid, return an empty string
    return ModsecBlobData.newBuilder()
        .setModsecBlob(
            modsecBlobValidator.validate(requestContext, combinedBlob, service) ? combinedBlob : "")
        .addAllRuleIds(
            modsecBlobResults.stream()
                .map(ModsecBlobResult::getRuleId)
                .filter(Predicate.not(String::isBlank))
                .collect(Collectors.toUnmodifiableList()))
        .build();
  }
}
