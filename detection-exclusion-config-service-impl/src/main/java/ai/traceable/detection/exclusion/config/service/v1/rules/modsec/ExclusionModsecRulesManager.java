package ai.traceable.detection.exclusion.config.service.v1.rules.modsec;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesResponse;
import ai.traceable.detection.exclusion.config.service.v1.ModsecBlobData;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecBlobConverter.ModsecBlobResult;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecClauseConverter.ModsecClauseResult;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ModsecClauseConverter.ServiceDetail;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    // Using same seed to keep modsec rules blob same if nothing else changes
    AtomicLong modsecIdAssignment = new AtomicLong(MODSEC_ID_SEED);

    // Map to hold service-specific ModSec blobs and rule IDs as pairs
    Map<String, List<ModsecBlobResult>> serviceToModsecBlobDataMap = new LinkedHashMap<>();
    // Ensure that all services have at least an empty blob
    serviceNames.forEach(
        service -> serviceToModsecBlobDataMap.computeIfAbsent(service, k -> new ArrayList<>()));
    List<DetectionExclusionModsecRule> exclusionModsecRules = new ArrayList<>();

    // Convert exclusion rule to modsec blob
    exclusionRules.forEach(
        exclusionRule -> {
          ModsecClauseResult modsecClauseResult =
              modsecClauseConverter.convert(requestContext, exclusionRule);
          ModsecBlobResult modsecBlobResult =
              modsecBlobConverter.convertToModsecRule(
                  exclusionRule.getId(), modsecClauseResult.getClauses(), modsecIdAssignment);
          List<String> servicesApplicable =
              servicesOnWhichRuleIsApplicable(modsecClauseResult.getServiceDetails(), serviceNames);

          // Store the modsecBlob and rule ID pair against all applicable services
          servicesApplicable.forEach(
              service -> serviceToModsecBlobDataMap.get(service).add(modsecBlobResult));

          // Convert into DetectionExclusionModsecRule
          if (modsecBlobResult.getModsecBlob().isBlank()) {
            // If blob is empty there would be no associated modsec rule id
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

    // Merge blobs across services with the same rules and convert to a list of ModsecBlobData
    List<ModsecBlobData> modsecBlobDataList =
        getModsecBlobData(requestContext, serviceToModsecBlobDataMap);

    return GetExclusionModsecRulesResponse.newBuilder()
        .setModsecDirectivesBlob(
            modsecRulesRegistry.getModsecHeader(
                ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE))
        .addAllModsecBlobsData(modsecBlobDataList)
        .addAllModsecRules(exclusionModsecRules)
        .build();
  }

  private List<ModsecBlobData> getModsecBlobData(
      RequestContext requestContext,
      Map<String, List<ModsecBlobResult>> serviceToModsecBlobDataMap) {
    // Using a linkedHashMap we don't want random re-ordering, as that would lead to change in hash
    // of the config, which will enforce an update of config in the agents.
    Map<String, ModsecBlobData.Builder> combinedBlobDataMap = new LinkedHashMap<>();

    serviceToModsecBlobDataMap.forEach(
        (service, modsecBlobResults) -> {
          final String combinedBlob =
              modsecBlobResults.stream()
                  .map(ModsecBlobResult::getModsecBlob)
                  .filter(Predicate.not(String::isBlank))
                  .collect(Collectors.joining(NEW_LINES_DELIMITER));

          // Validate the combined blob; if invalid, set it to an empty string
          if (modsecBlobValidator.validate(requestContext, combinedBlob, service)) {
            List<String> ruleIds =
                modsecBlobResults.stream()
                    .map(ModsecBlobResult::getRuleId)
                    .filter(Predicate.not(String::isBlank))
                    .collect(Collectors.toUnmodifiableList());

            // Merge or create a new entry for the combined blob
            combinedBlobDataMap
                .computeIfAbsent(
                    combinedBlob,
                    k ->
                        ModsecBlobData.newBuilder()
                            .setModsecBlob(combinedBlob)
                            .addAllRuleIds(ruleIds))
                .addServiceNames(service);
          }
        });

    // Convert the combinedBlobDataMap into a list of ModsecBlobData
    return combinedBlobDataMap.values().stream()
        .map(ModsecBlobData.Builder::build)
        .collect(Collectors.toUnmodifiableList());
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
}
