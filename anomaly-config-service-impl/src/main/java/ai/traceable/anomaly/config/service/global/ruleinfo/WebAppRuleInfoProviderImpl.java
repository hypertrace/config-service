package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.v1.AnomalyEventDetails;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySeverityLevel;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import ai.traceable.protection.processor.secrules.v1.CorazaRuleDirectivesType;
import ai.traceable.protection.processor.secrules.v1.ModsecJniRuleDirectivesType;
import ai.traceable.protection.processor.secrules.v1.impl.utils.SecRulesUtils;
import ai.traceable.protection.rules.webapp.v1.WebAppProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesData;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesVersion;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesVersionType;
import ai.traceable.protection.rules.webapp.v1.WebAppSeverity;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatDetails;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatLabel;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRule;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleDefinition;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleType;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatType;
import ai.traceable.protection.rules.webapp.v1.WebAppVersionedRules;
import ai.traceable.protection.rules.webapp.v1.WebAppVersionedRulesFilter;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebAppRuleInfoProviderImpl implements WebAppRuleInfoProvider {
  private final WebAppProtectionRulesProvider webAppProtectionRulesProvider;
  private final ModsecRuleUtils modsecRuleUtils;
  private static final String SEC_RULE_REMOVE_BY_ID_FORMAT = "SecRuleRemoveById %d";
  private static final String EMPTY_STRING = "";
  private static final String NEWLINE_DELIMITER = "\n";
  private static final String CORAZA_DIRECTIVES_DEFAULT_FILE_PATH = "directives/coraza-basic.conf";
  private static final String MODSEC_JNI_DIRECTIVES_DEFAULT_FILE_PATH =
      "directives/modsec-basic.conf";

  @Inject
  public WebAppRuleInfoProviderImpl(
      WebAppProtectionRulesProvider webAppProtectionRulesProvider,
      ModsecRuleUtils modsecRuleUtils) {
    this.webAppProtectionRulesProvider = webAppProtectionRulesProvider;
    this.modsecRuleUtils = modsecRuleUtils;
  }

  @Override
  public List<AnomalyRuleInfo> getWebAppRuleInfo(RuleVersion version) {
    return new ArrayList<>(
        convertToAnomalyRuleInfos(getWebAppVersionedRules(convertToWebAppRulesVersion(version))));
  }

  @Override
  public String getCrsRulesBlob(
      List<AnomalySubRuleType> subRuleTypes,
      ModsecRuleVersion ruleVersion,
      Set<String> disabledRuleIds,
      RuleVersion version,
      boolean includeDirectives) {
    WebAppRulesData rulesData =
        getWebAppVersionedRules(convertToWebAppRulesVersion(version)).getRulesData();
    String crsRulesBlob = rulesData.getCrsRulesBlob();
    List<WebAppThreatRule> webAppThreatRules = rulesData.getThreatRulesList();

    // Ids to be removed should be ordered to ensure the ModSec blob doesn't keep changing on
    // subsequent calls.
    Set<String> idsToBeRemoved = new TreeSet<>();

    webAppThreatRules.stream()
        .filter(webAppThreatRule -> removedRule(webAppThreatRule, subRuleTypes, disabledRuleIds))
        .forEach(
            anomalySubRuleInfo ->
                idsToBeRemoved.add(
                    String.format(
                        SEC_RULE_REMOVE_BY_ID_FORMAT,
                        modsecRuleUtils.getModsecCrsRuleIdNumber(
                            anomalySubRuleInfo.getThreatRuleId()))));

    if (webAppThreatRules.size() == idsToBeRemoved.size()) {
      return EMPTY_STRING;
    }
    if (includeDirectives) {
      return String.join(
          NEWLINE_DELIMITER,
          getDirectivesString(ruleVersion),
          crsRulesBlob,
          String.join(NEWLINE_DELIMITER, idsToBeRemoved));
    }

    return String.join(
        NEWLINE_DELIMITER, crsRulesBlob, String.join(NEWLINE_DELIMITER, idsToBeRemoved));
  }

  private WebAppVersionedRules getWebAppVersionedRules(WebAppRulesVersion version) {
    WebAppVersionedRulesFilter filter =
        WebAppVersionedRulesFilter.newBuilder()
            .addRulesVersions(version.getVersion())
            .addRulesVersionTypes(version.getVersionType())
            .build();
    return webAppProtectionRulesProvider.getWebAppVersionedRules(filter).get(0);
  }

  private List<AnomalyRuleInfo> convertToAnomalyRuleInfos(WebAppVersionedRules versionedRules) {

    WebAppRulesData rulesData = versionedRules.getRulesData();
    List<AnomalyRuleInfo> ruleInfos = new ArrayList<>();
    Map<String, List<WebAppThreatRule>> rulesByTypeId = new HashMap<>();

    for (WebAppThreatRule rule : rulesData.getThreatRulesList()) {
      String typeId = rule.getRuleDefinition().getThreatTypeId();
      rulesByTypeId.computeIfAbsent(typeId, k -> new ArrayList<>()).add(rule);
    }
    for (WebAppThreatType threatType : rulesData.getThreatTypesList()) {
      String typeId = threatType.getTypeId();
      List<WebAppThreatRule> rulesForType = rulesByTypeId.get(typeId);
      WebAppThreatDetails threatDetails = threatType.getThreatDetails();
      AnomalyRuleInfo.Builder anomalyRuleInfoBuilder =
          AnomalyRuleInfo.newBuilder()
              .setRuleId(typeId)
              .setRuleName(threatType.getTypeName())
              .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
              .putAllEventLabels(convertEventLabels(threatType.getThreatLabels().getLabelsList()))
              .setEventDetails(createEventDetails(threatDetails));

      for (WebAppThreatRule rule : rulesForType) {
        WebAppThreatRuleDefinition ruleDef = rule.getRuleDefinition();
        anomalyRuleInfoBuilder.addSubRuleInfos(
            AnomalySubRuleInfo.newBuilder()
                .setRuleId(rule.getThreatRuleId())
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

  private AnomalyEventDetails createEventDetails(WebAppThreatDetails threatDetails) {
    return AnomalyEventDetails.newBuilder()
        .setDescription(threatDetails.getDescription())
        .setMitigation(threatDetails.getMitigation())
        .setImpact(threatDetails.getImpact())
        .setReferences(threatDetails.getReferences())
        .build();
  }

  private WebAppRulesVersion convertToWebAppRulesVersion(RuleVersion ruleVersion) {
    WebAppRulesVersionType versionType;
    switch (ruleVersion.getVersionType()) {
      case RULE_VERSION_TYPE_STABLE:
        versionType = WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_STABLE;
        break;
      case RULE_VERSION_TYPE_BETA:
        versionType = WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_BETA;
        break;
      case RULE_VERSION_TYPE_EXPERIMENTAL:
        versionType = WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_EXPERIMENTAL;
        break;
      case RULE_VERSION_TYPE_DEPRECATED:
        versionType = WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_DEPRECATED;
        break;
      default:
        versionType = WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_UNSPECIFIED;
    }

    return WebAppRulesVersion.newBuilder()
        .setVersion(ruleVersion.getVersion())
        .setVersionType(versionType)
        .build();
  }

  private AnomalySeverityLevel convertToAnomalySeverityLevel(WebAppSeverity severity) {
    switch (severity) {
      case WEB_APP_SEVERITY_LOW:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_LOW;
      case WEB_APP_SEVERITY_MEDIUM:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_MEDIUM;
      case WEB_APP_SEVERITY_HIGH:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_HIGH;
      case WEB_APP_SEVERITY_CRITICAL:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_CRITICAL;
      default:
        return AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_UNSPECIFIED;
    }
  }

  private Map<String, String> convertEventLabels(List<WebAppThreatLabel> labels) {
    Map<String, String> labelMap = new HashMap<>();
    for (WebAppThreatLabel label : labels) {
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

  private List<AnomalySubRuleType> convertSubRuleTypes(WebAppThreatRuleType webAppThreatRuleType) {
    switch (webAppThreatRuleType) {
      case WEB_APP_THREAT_RULE_TYPE_STANDARD:
        return List.of(
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK,
            AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE);
      case WEB_APP_THREAT_RULE_TYPE_AGGRESSIVE:
        return List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);
      default:
        throw new IllegalArgumentException(
            "Unsupported WebAppThreatRuleType: " + webAppThreatRuleType);
    }
  }

  private boolean removedRule(
      WebAppThreatRule webAppThreatRule,
      List<AnomalySubRuleType> subRuleTypes,
      Set<String> disabledRuleIds) {
    if (disabledRuleIds.contains(webAppThreatRule.getThreatRuleId())) {
      return true;
    }
    return !subRuleTypes.isEmpty()
        && convertSubRuleTypes(webAppThreatRule.getRuleDefinition().getRuleType()).stream()
            .noneMatch(subRuleTypes::contains);
  }

  private static String getDirectivesString(ModsecRuleVersion modsecRuleVersion) {
    switch (modsecRuleVersion) {
      case MODSEC_RULE_VERSION_V3:
      case MODSEC_RULE_VERSION_TEST_V3:
      case MODSEC_RULE_VERSION_SENSITIVE_AGENT_V3:
        return SecRulesUtils.loadModsecFileContents(
            SecRulesUtils.getModSecJniRuleDirectiveFilePath(
                ModsecJniRuleDirectivesType.MODSEC_JNI_RULE_DIRECTIVES_TYPE_BASIC,
                MODSEC_JNI_DIRECTIVES_DEFAULT_FILE_PATH));
      case MODSEC_RULE_VERSION_V3_SECARG_LIMITS:
      case MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS:
        return SecRulesUtils.loadModsecFileContents(
            SecRulesUtils.getModSecJniRuleDirectiveFilePath(
                ModsecJniRuleDirectivesType.MODSEC_JNI_RULE_DIRECTIVES_TYPE_SECARGLIMITS,
                MODSEC_JNI_DIRECTIVES_DEFAULT_FILE_PATH));
      case MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
      case MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
        return SecRulesUtils.loadModsecFileContents(
            SecRulesUtils.getModSecJniRuleDirectiveFilePath(
                ModsecJniRuleDirectivesType
                    .MODSEC_JNI_RULE_DIRECTIVES_TYPE_SECARGLIMITS_DETECTION_ONLY,
                MODSEC_JNI_DIRECTIVES_DEFAULT_FILE_PATH));
      case MODSEC_RULE_VERSION_CORAZA_V3:
      case MODSEC_RULE_VERSION_TEST_CORAZA_V3:
      case MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3:
        return SecRulesUtils.loadModsecFileContents(
            SecRulesUtils.getCorazaRuleDirectiveFilePath(
                CorazaRuleDirectivesType.CORAZA_RULE_DIRECTIVES_TYPE_BASIC,
                CORAZA_DIRECTIVES_DEFAULT_FILE_PATH));
      case MODSEC_RULE_VERSION_CORAZA_V3_DETECTION_ONLY_MODE:
      case MODSEC_RULE_VERSION_TEST_CORAZA_V3_DETECTION_ONLY_MODE:
        return SecRulesUtils.loadModsecFileContents(
            SecRulesUtils.getCorazaRuleDirectiveFilePath(
                CorazaRuleDirectivesType.CORAZA_RULE_DIRECTIVES_TYPE_DETECTION_ONLY,
                CORAZA_DIRECTIVES_DEFAULT_FILE_PATH));
      default:
        throw new IllegalArgumentException(
            "Unsupported ModsecRuleVersion: " + modsecRuleVersion.name());
    }
  }
}
