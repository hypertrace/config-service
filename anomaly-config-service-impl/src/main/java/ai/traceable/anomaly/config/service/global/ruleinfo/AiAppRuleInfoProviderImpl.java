package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.v1.*;
import ai.traceable.protection.rules.aiapp.v1.*;
import ai.traceable.protection.rules.webapp.v1.*;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class AiAppRuleInfoProviderImpl implements AiAppRuleInfoProvider {

  private final AiAppRulesProvider rulesProvider;

  @Override
  public List<AnomalyRuleInfo> getAiAppRuleInfo() {
    return convertToAnomalyRuleInfos(rulesProvider.getAiAppRules());
  }

  private List<AnomalyRuleInfo> convertToAnomalyRuleInfos(AiAppRules aiAppRules) {

    List<AnomalyRuleInfo> ruleInfos = new ArrayList<>();
    Map<String, List<AiAppThreatRule>> rulesByTypeId = new HashMap<>();

    for (AiAppThreatRule rule : aiAppRules.getThreatRulesList()) {
      String typeId = rule.getRuleDefinition().getThreatTypeId();
      rulesByTypeId.computeIfAbsent(typeId, k -> new ArrayList<>()).add(rule);
    }
    for (AiAppThreatType threatType : aiAppRules.getThreatTypesList()) {
      String typeId = threatType.getTypeId();
      List<AiAppThreatRule> rulesForType = rulesByTypeId.get(typeId);
      AiAppThreatDetails threatDetails = threatType.getThreatDetails();
      AnomalyRuleInfo.Builder anomalyRuleInfoBuilder =
          AnomalyRuleInfo.newBuilder()
              .setRuleId(typeId)
              .setRuleName(threatType.getTypeName())
              .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_GEN_AI)
              .putAllEventLabels(convertEventLabels(threatType.getThreatLabels().getLabelsList()))
              .setEventDetails(createEventDetails(threatDetails));

      for (AiAppThreatRule rule : rulesForType) {
        AiAppThreatRuleDefinition ruleDef = rule.getRuleDefinition();
        anomalyRuleInfoBuilder.addSubRuleInfos(
            AnomalySubRuleInfo.newBuilder()
                .setRuleId(rule.getRuleId())
                .setRuleName(ruleDef.getRuleName())
                .setSeverityLevel(convertToAnomalySeverityLevel(ruleDef.getSeverity()))
                .putAllEventLabels(convertEventLabels(ruleDef.getThreatLabels().getLabelsList()))
                .setEventDetails(createEventDetails(rule.getThreatDetails()))
                .build());
      }

      ruleInfos.add(anomalyRuleInfoBuilder.build());
    }

    return ruleInfos;
  }

  private AnomalyEventDetails createEventDetails(AiAppThreatDetails threatDetails) {
    return AnomalyEventDetails.newBuilder()
        .setDescription(threatDetails.getDescription())
        .setMitigation(threatDetails.getMitigation())
        .setImpact(threatDetails.getImpact())
        .setReferences(threatDetails.getReferences())
        .build();
  }

  private AnomalySeverityLevel convertToAnomalySeverityLevel(AiAppSeverity severity) {
    switch (severity) {
      case AI_APP_SEVERITY_LOW:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_LOW;
      case AI_APP_SEVERITY_MEDIUM:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_MEDIUM;
      case AI_APP_SEVERITY_HIGH:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_HIGH;
      case AI_APP_SEVERITY_CRITICAL:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_CRITICAL;
      default:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_UNSPECIFIED;
    }
  }

  private Map<String, String> convertEventLabels(List<AiAppThreatLabel> labels) {
    Map<String, String> labelMap = new HashMap<>();
    for (AiAppThreatLabel label : labels) {
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
        case OWASP_LLM_2025:
          value = label.getOwaspLlm2025();
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
}
