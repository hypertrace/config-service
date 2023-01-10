package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModsecRulesRegistryImpl implements ModsecRulesRegistry {
  private static final String NEWLINE_DELIMITER = "\n";
  private static final String MODSEC_DIRECTORY = "modsec/";
  private static final String MODSEC_RULE_DETAILS_FILE_PATH =
      MODSEC_DIRECTORY + "modsec-rule-details.conf";

  private static final String MODSEC_RULES_CONFIG_KEY = "modsecRules";
  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.01);

  private static final List<AnomalySubRuleType> SUPPORTED_SUB_RULE_TYPES =
      ImmutableList.of(
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);

  private final ConfigConverter configConverter;
  private final ModsecCrsRulesHandler modsecCrsRulesHandler;

  private final Map<String, AnomalyRuleInfo> modsecRules;
  private final Config modsecRuleDetails;

  @Inject
  public ModsecRulesRegistryImpl(
      ConfigConverter configConverter, ModsecCrsRulesHandler modsecCrsRulesHandler) {
    this.configConverter = configConverter;
    this.modsecCrsRulesHandler = modsecCrsRulesHandler;
    try {
      modsecRuleDetails = ConfigFactory.parseResources(MODSEC_RULE_DETAILS_FILE_PATH);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to read modsec rule details file: %s", MODSEC_RULE_DETAILS_FILE_PATH),
          e);
    }
    this.modsecRules = initModsecRules();
  }

  @Override
  public Map<String, AnomalyRuleInfo> getModsecRuleInfos() {
    return modsecRules;
  }

  @Override
  public String getModsecCrsRulesBlob(
      AnomalySubRuleType subRuleType,
      ModsecRuleVersion ruleVersion,
      Set<String> disabledModsecRuleIds) {
    if (!SUPPORTED_SUB_RULE_TYPES.contains(subRuleType)) {
      throw new IllegalArgumentException(
          String.format("Invalid SubRuleType %s to fetch Modsec CRS rules", subRuleType));
    }
    ModsecCrsConfig modsecCrsConfig = getModsecCrsConfig(ruleVersion);
    return modsecCrsRulesHandler.getModsecCrsBlob(
        modsecCrsConfig.getDirectivesFilePath(),
        modsecCrsConfig.getInitializationRulesFilePath(),
        modsecCrsConfig.getRulesFilePath(),
        modsecRules,
        subRuleType,
        disabledModsecRuleIds);
  }

  @Override
  public String getModsecHeader(ModsecRuleVersion ruleVersion) {
    return String.join(
        NEWLINE_DELIMITER,
        modsecCrsRulesHandler.loadModsecCrsFileContents(
            getModsecCrsConfig(ruleVersion).getDirectivesFilePath()),
        modsecCrsRulesHandler.loadModsecCrsFileContents(
            getModsecCrsConfig(ruleVersion).getInitializationRulesFilePath()));
  }

  private ModsecCrsConfig getModsecCrsConfig(ModsecRuleVersion ruleVersion) {
    ModsecCrsConfig crsConfig = ModsecCrsConfig.ruleVersionToConfigMap.get(ruleVersion);
    if (crsConfig == null) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.error(
            "Cannot get modsec crs configs for requested version : {}, defaulting to MODSEC_CRS_V3_CONFIG",
            ruleVersion);
      }
      return ModsecCrsConfig.MODSEC_CRS_V3_CONFIG;
    }
    return crsConfig;
  }

  private Map<String, AnomalyRuleInfo> initModsecRules() {
    Map<String, AnomalyRuleInfo> mergedRuleInfos = new HashMap<>();
    for (ModsecRuleVersion ruleVersion : ModsecRuleVersion.values()) {
      Map<String, AnomalyRuleInfo> versionRuleInfos = initModsecRulesForRuleVersion(ruleVersion);
      versionRuleInfos.forEach(
          (id, ruleInfo) -> mergedRuleInfos.merge(id, ruleInfo, this::mergeAnomalyRuleInfos));
    }
    return mergedRuleInfos;
  }

  AnomalyRuleInfo mergeAnomalyRuleInfos(AnomalyRuleInfo v1, AnomalyRuleInfo v2) {
    // for a given rule id merge subRule infos, assuming all non id fields match
    // same in case of subRule info comparison, if id is same assume all others match
    Map<String, AnomalySubRuleInfo> mergedSubRuleInfos = new HashMap<>();
    v1.getSubRuleInfosList()
        .forEach(subRuleInfo -> mergedSubRuleInfos.put(subRuleInfo.getRuleId(), subRuleInfo));
    v2.getSubRuleInfosList()
        .forEach(
            otherSubRuleInfo ->
                mergedSubRuleInfos.merge(
                    otherSubRuleInfo.getRuleId(),
                    otherSubRuleInfo,
                    (subRuleInfo1, subRuleInfo2) ->
                        subRuleInfo1.toBuilder().mergeFrom(subRuleInfo2).build()));
    return v1.toBuilder()
        .clearSubRuleInfos()
        .addAllSubRuleInfos(mergedSubRuleInfos.values())
        .build();
  }

  private Map<String, AnomalyRuleInfo> initModsecRulesForRuleVersion(
      ModsecRuleVersion ruleVersion) {
    if (!ModsecCrsConfig.ruleVersionToConfigMap.containsKey(ruleVersion)) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.warn("Cannot get rule files for: {}, returning empty ruleinfo map", ruleVersion);
      }
      return Collections.emptyMap();
    }
    Map<String, AnomalyRuleInfo.Builder> anomalyRuleBuildersMap = new HashMap<>();

    configConverter
        .convertAnomalyRuleInfos(
            modsecRuleDetails.getConfigList(MODSEC_RULES_CONFIG_KEY),
            AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
        .forEach((id, rule) -> anomalyRuleBuildersMap.put(id, rule.toBuilder()));

    modsecCrsRulesHandler
        .parseModsecCrsRules(
            modsecCrsRulesHandler.loadModsecCrsFileContents(
                getModsecCrsConfig(ruleVersion).rulesFilePath))
        .forEach((id, subRules) -> anomalyRuleBuildersMap.get(id).addAllSubRuleInfos(subRules));

    return anomalyRuleBuildersMap.values().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                AnomalyRuleInfo.Builder::getRuleId, AnomalyRuleInfo.Builder::build));
  }
}
