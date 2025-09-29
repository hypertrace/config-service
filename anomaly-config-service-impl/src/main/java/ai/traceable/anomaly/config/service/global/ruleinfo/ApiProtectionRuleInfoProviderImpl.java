package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.v1.AnomalyEventDetails;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySeverityLevel;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesData;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesVersion;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesVersionType;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectSeverity;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatDetails;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatLabel;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRule;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRuleDefinition;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRuleType;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatType;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectVersionedRules;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectVersionedRulesFilter;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectionRulesProvider;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ApiProtectionRuleInfoProviderImpl implements ApiProtectionRuleInfoProvider {

  private final ApiProtectionRulesProvider apiProtectionRulesProvider;
  private static final String API_PROTECT_RULE_VERSION = "1.0.0";

  private final Map<AnomalyEventFamily, Map<String, Set<String>>>
      eventFamilyToThreatTypeIdsByVersion =
          new HashMap<>(
              Map.of(
                  AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF,
                  Map.of(
                      API_PROTECT_RULE_VERSION,
                      Set.of(
                          "jwt",
                          "unknownParam",
                          "enum",
                          "integer",
                          "type",
                          "missingParam",
                          "specialCharacter",
                          "contentExplosion",
                          "device",
                          "httpStatus",
                          "contentType",
                          "contentSize",
                          "ssrf",
                          "bfla")),
                  AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION,
                  Map.of(API_PROTECT_RULE_VERSION, Set.of("sessionv", "bola", "userIdBola")),
                  AnomalyEventFamily.ANOMALY_EVENT_FAMILY_VOLUMETRIC,
                  Map.of(API_PROTECT_RULE_VERSION, Set.of("volumetric")),
                  AnomalyEventFamily.ANOMALY_EVENT_FAMILY_CREDENTIAL_STUFFING,
                  Map.of(API_PROTECT_RULE_VERSION, Set.of("ato"))));

  @Inject
  public ApiProtectionRuleInfoProviderImpl(ApiProtectionRulesProvider apiProtectionRulesProvider) {
    this.apiProtectionRulesProvider = apiProtectionRulesProvider;
  }

  @Override
  public List<AnomalyRuleInfo> getApiProtectRuleInfo(
      RuleVersion version, AnomalyEventFamily anomalyEventFamily) {
    return new ArrayList<>(
        convertToAnomalyRuleInfos(
            getApiProtectVersionedRules(convertToApiProtectRulesVersion(version)),
            anomalyEventFamily));
  }

  private ApiProtectRulesVersion convertToApiProtectRulesVersion(RuleVersion ruleVersion) {
    ApiProtectRulesVersionType versionType;
    switch (ruleVersion.getVersionType()) {
      case RULE_VERSION_TYPE_STABLE:
        versionType = ApiProtectRulesVersionType.API_PROTECT_RULES_VERSION_TYPE_STABLE;
        break;
      case RULE_VERSION_TYPE_BETA:
        versionType = ApiProtectRulesVersionType.API_PROTECT_RULES_VERSION_TYPE_BETA;
        break;
      case RULE_VERSION_TYPE_DEPRECATED:
        versionType = ApiProtectRulesVersionType.API_PROTECT_RULES_VERSION_TYPE_DEPRECATED;
        break;
      default:
        versionType = ApiProtectRulesVersionType.API_PROTECT_RULES_VERSION_TYPE_UNSPECIFIED;
    }

    return ApiProtectRulesVersion.newBuilder()
        .setVersion(ruleVersion.getVersion())
        .setVersionType(versionType)
        .build();
  }

  private ApiProtectVersionedRules getApiProtectVersionedRules(ApiProtectRulesVersion version) {
    ApiProtectVersionedRulesFilter filter =
        ApiProtectVersionedRulesFilter.newBuilder()
            .addRulesVersionTypes(version.getVersionType())
            .addRuleVersions(version.getVersion())
            .build();
    return apiProtectionRulesProvider.getApiProtectVersionedRules(filter).get(0);
  }

  private List<AnomalyRuleInfo> convertToAnomalyRuleInfos(
      ApiProtectVersionedRules versionedRules, AnomalyEventFamily anomalyEventFamily) {

    ApiProtectRulesData rulesData = versionedRules.getRulesData();
    List<AnomalyRuleInfo> ruleInfos = new ArrayList<>();
    Map<String, List<ApiProtectThreatRule>> rulesByTypeId = new HashMap<>();

    for (ApiProtectThreatRule rule : rulesData.getThreatRulesList()) {
      String typeId = rule.getRuleDefinition().getThreatTypeId();
      rulesByTypeId.computeIfAbsent(typeId, k -> new ArrayList<>()).add(rule);
    }

    Map<String, Set<String>> versionToRuleTypeIds =
        eventFamilyToThreatTypeIdsByVersion.get(anomalyEventFamily);
    if (versionToRuleTypeIds == null) {
      throw new IllegalArgumentException("Unsupported AnomalyEventFamily: " + anomalyEventFamily);
    }
    Set<String> relevantTypeIds =
        versionToRuleTypeIds.get(versionedRules.getRulesVersion().getVersion());
    for (ApiProtectThreatType threatType : rulesData.getThreatTypesList()) {
      String typeId = threatType.getTypeId();

      if (relevantTypeIds == null || !relevantTypeIds.contains(typeId)) {
        continue;
      }

      List<ApiProtectThreatRule> rulesForType = rulesByTypeId.get(typeId);
      if (rulesForType == null) continue;

      ApiProtectThreatDetails threatDetails = threatType.getThreatDetails();
      AnomalyRuleInfo.Builder anomalyRuleInfoBuilder =
          AnomalyRuleInfo.newBuilder()
              .setRuleId(typeId)
              .setRuleName(threatType.getTypeName())
              .setEventFamily(anomalyEventFamily)
              .putAllEventLabels(convertEventLabels(threatType.getThreatLabels().getLabelsList()))
              .setEventDetails(createEventDetails(threatDetails));

      for (ApiProtectThreatRule rule : rulesForType) {
        ApiProtectThreatRuleDefinition ruleDef = rule.getRuleDefinition();
        anomalyRuleInfoBuilder.addSubRuleInfos(
            AnomalySubRuleInfo.newBuilder()
                .setRuleId(rule.getRuleId())
                .setRuleName(ruleDef.getRuleName())
                .addAllSubRuleTypes(convertSubRuleTypes(ruleDef.getRuleType()))
                .setSeverityLevel(convertToAnomalySeverityLevel(ruleDef.getSeverity()))
                .putAllEventLabels(convertEventLabels(ruleDef.getThreatLabels().getLabelsList()))
                .setEventDetails(createEventDetails(rule.getThreatDetails()))
                .build());
      }

      ruleInfos.add(anomalyRuleInfoBuilder.build());
    }

    return ruleInfos;
  }

  private AnomalySeverityLevel convertToAnomalySeverityLevel(ApiProtectSeverity severity) {
    switch (severity) {
      case API_PROTECT_SEVERITY_LOW:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_LOW;
      case API_PROTECT_SEVERITY_MEDIUM:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_MEDIUM;
      case API_PROTECT_SEVERITY_HIGH:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_HIGH;
      case API_PROTECT_SEVERITY_CRITICAL:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_CRITICAL;
      default:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_UNSPECIFIED;
    }
  }

  private Map<String, String> convertEventLabels(List<ApiProtectThreatLabel> labels) {
    Map<String, String> labelMap = new HashMap<>();
    for (ApiProtectThreatLabel label : labels) {
      String key = label.getLabelCase().name().toUpperCase();
      String value;
      switch (label.getLabelCase()) {
        case OWASP_API_2023:
          value = label.getOwaspApi2023();
          break;
        case OWASP_2021:
          value = label.getOwasp2021();
          break;
        case OWASP_API_2019:
          value = label.getOwaspApi2019();
          break;
        case OWASP_2017:
          value = label.getOwasp2017();
          break;
        case CWE:
          value = label.getCwe();
          break;
        case CVE:
          value = label.getCve();
          break;
        case MITRE_TACTIC:
          value = label.getMitreTactic();
          break;
        case MITRE_TECHNIQUE:
          value = label.getMitreTechnique();
          break;
        case CUSTOM_LABEL:
          value = label.getCustomLabel().getValue();
          key = label.getCustomLabel().getKey();
          break;
        default:
          continue;
      }
      labelMap.put(key, value);
    }
    return labelMap;
  }

  private AnomalyEventDetails createEventDetails(ApiProtectThreatDetails threatDetails) {
    return AnomalyEventDetails.newBuilder()
        .setDescription(threatDetails.getDescription())
        .setMitigation(threatDetails.getMitigation())
        .setImpact(threatDetails.getImpact())
        .setReferences(threatDetails.getReferences())
        .build();
  }

  private List<AnomalySubRuleType> convertSubRuleTypes(
      ApiProtectThreatRuleType apiProtectThreatRuleType) {
    switch (apiProtectThreatRuleType) {
      case API_PROTECT_THREAT_RULE_TYPE_STANDARD:
        return List.of(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK,
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
      case API_PROTECT_THREAT_RULE_TYPE_ANOMALY:
        return List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);
      default:
        throw new IllegalArgumentException(
            "Unsupported ApiProtectThreatRuleType: " + apiProtectThreatRuleType);
    }
  }
}
