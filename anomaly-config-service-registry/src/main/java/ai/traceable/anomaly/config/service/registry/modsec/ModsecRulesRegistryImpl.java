package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModsecRulesRegistryImpl implements ModsecRulesRegistry {
  private static final String NEWLINE_DELIMITER = "\n";
  private static final String MODSEC_DIRECTORY = "modsec/";
  private static final String MODSEC_RULE_DETAILS_FILE_PATH =
      MODSEC_DIRECTORY + "modsec-rule-details.yaml";

  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.01);

  private static final List<AnomalySubRuleType> SUPPORTED_SUB_RULE_TYPES =
      ImmutableList.of(
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_UNSAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK);

  private final ConfigConverter configConverter;
  private final ModsecCrsRulesHandler modsecCrsRulesHandler;

  private Map<ModsecRuleVersion, Map<String, AnomalyRuleInfo>> versionedModsecRules;

  @Inject
  public ModsecRulesRegistryImpl(
      ConfigConverter configConverter, ModsecCrsRulesHandler modsecCrsRulesHandler) {
    this.configConverter = configConverter;
    this.modsecCrsRulesHandler = modsecCrsRulesHandler;
    initAnomalyRulesInfoMap();
  }

  @Override
  public Map<String, AnomalyRuleInfo> getModsecRuleInfos(ModsecRuleVersion modsecRuleVersion) {
    return versionedModsecRules.get(modsecRuleVersion);
  }

  @Override
  public String getModsecCrsRulesBlob(
      List<AnomalySubRuleType> subRuleTypes,
      ModsecRuleVersion ruleVersion,
      Set<String> disabledModsecRuleIds) {
    ModsecCrsConfig modsecCrsConfig = getModsecCrsConfig(ruleVersion);
    return modsecCrsRulesHandler.getModsecCrsBlob(
        modsecCrsConfig.getDirectivesFilePath(),
        modsecCrsConfig.getInitializationRulesFilePath(),
        modsecCrsConfig.getRulesFilePath(),
        versionedModsecRules.get(ruleVersion),
        subRuleTypes,
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
      return ModsecCrsConfig.ruleVersionToConfigMap.get(ModsecRuleVersion.MODSEC_RULE_VERSION_V3);
    }
    return crsConfig;
  }

  private void initAnomalyRulesInfoMap() {
    Map<String, AnomalyRuleInfo> anomalyRulesInfoMap =
        new HashMap<>(
            configConverter.getAnomalyRuleInfos(
                MODSEC_RULE_DETAILS_FILE_PATH, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC));

    Map<String, AnomalyRuleInfo> allMergedModsecRules = new HashMap<>(anomalyRulesInfoMap);
    versionedModsecRules = new HashMap<>();

    for (ModsecRuleVersion ruleVersion : ModsecRuleVersion.values()) {
      if (ruleVersion.equals(ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED)
          || ruleVersion.equals(ModsecRuleVersion.UNRECOGNIZED)) {
        continue;
      }
      if (!ModsecCrsConfig.ruleVersionToConfigMap.containsKey(ruleVersion)) {
        throw new IllegalArgumentException(
            String.format("Cannot get modsec rule files for: %s", ruleVersion));
      } else {
        Map<String, List<AnomalySubRuleInfo>> modsecRulesMap =
            modsecCrsRulesHandler.parseModsecCrsRules(
                modsecCrsRulesHandler.loadModsecCrsFileContents(
                    getModsecCrsConfig(ruleVersion).rulesFilePath));
        versionedModsecRules.put(
            ruleVersion, mergeAnomalyRuleInfos(anomalyRulesInfoMap, modsecRulesMap));
        allMergedModsecRules = mergeAnomalyRuleInfos(allMergedModsecRules, modsecRulesMap);
      }
    }
    versionedModsecRules.put(
        ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED, allMergedModsecRules);
    versionedModsecRules.put(ModsecRuleVersion.UNRECOGNIZED, allMergedModsecRules);
  }

  private Map<String, AnomalyRuleInfo> mergeAnomalyRuleInfos(
      Map<String, AnomalyRuleInfo> anomalyRulesInfoMap,
      Map<String, List<AnomalySubRuleInfo>> modsecRulesMap) {
    Map<String, AnomalyRuleInfo> result = new HashMap<>(anomalyRulesInfoMap);
    modsecRulesMap.forEach(
        (ruleId, subRules) -> {
          if (result.containsKey(ruleId)) {
            result.put(
                ruleId,
                mergeAnomalyRuleInfos(
                    result.get(ruleId),
                    AnomalyRuleInfo.newBuilder()
                        .setRuleId(ruleId)
                        .addAllSubRuleInfos(subRules)
                        .build()));
          }
        });
    return result;
  }

  private AnomalyRuleInfo mergeAnomalyRuleInfos(AnomalyRuleInfo v1, AnomalyRuleInfo v2) {
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
                    (subRuleInfo1, subRuleInfo2) -> {
                      AnomalySubRuleInfo.Builder builder =
                          subRuleInfo1.toBuilder().mergeFrom(subRuleInfo2);
                      List<AnomalySubRuleType> subRuleTypes =
                          builder.getSubRuleTypesList().stream()
                              .distinct()
                              .collect(Collectors.toList());
                      builder.clearSubRuleTypes().addAllSubRuleTypes(subRuleTypes);
                      return builder.build();
                    }));
    return v1.toBuilder()
        .clearSubRuleInfos()
        .addAllSubRuleInfos(mergedSubRuleInfos.values())
        .build();
  }
}
