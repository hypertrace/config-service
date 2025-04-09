package ai.traceable.anomaly.config.service.registry.modsec;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.google.common.util.concurrent.RateLimiter;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModsecRulesRegistryImpl implements ModsecRulesRegistry {
  private static final String NEWLINE_DELIMITER = "\n";
  private static final String MODSEC_DIRECTORY = "modsec/";
  private static final String MODSEC_RULE_DETAILS_FILE_PATH =
      MODSEC_DIRECTORY + "modsec-rule-details.yaml";

  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.01);

  private final ConfigConverter configConverter;
  private final ModsecCrsRulesHandler modsecCrsRulesHandler;

  private Map<ModsecRuleVersion, Map<String, AnomalyRuleInfo>> versionedModsecRules;
  private Map<ModsecRuleVersion, Map<String, AnomalyRuleInfo>> versionedTestModsecRules;

  @Inject
  public ModsecRulesRegistryImpl(
      ConfigConverter configConverter, ModsecCrsRulesHandler modsecCrsRulesHandler) {
    this.configConverter = configConverter;
    this.modsecCrsRulesHandler = modsecCrsRulesHandler;
    initAnomalyRulesInfoMap();
  }

  @Override
  public Map<String, AnomalyRuleInfo> getModsecRuleInfos(
      ModsecRuleVersion modsecRuleVersion, boolean useTestRules) {
    ModsecRuleVersion ruleVersion = getTestStrippedVersion(modsecRuleVersion);
    // handle deprecated enums
    useTestRules = useTestRules || isModsecTestRuleVersion(modsecRuleVersion);
    return useTestRules
        ? versionedTestModsecRules.get(ruleVersion)
        : versionedModsecRules.get(ruleVersion);
  }

  @Override
  public String getModsecCrsRulesBlob(
      List<AnomalySubRuleType> subRuleTypes,
      ModsecRuleVersion ruleVersion,
      Set<String> disabledModsecRuleIds,
      boolean useTestRules) {
    ModsecCrsConfig modsecCrsConfig = getModsecCrsConfig(ruleVersion);
    // handle deprecated enums
    useTestRules = useTestRules || isModsecTestRuleVersion(ruleVersion);
    return modsecCrsRulesHandler.getModsecCrsBlob(
        modsecCrsConfig.getDirectivesFilePath(),
        modsecCrsConfig.getInitializationRulesFilePath(),
        useTestRules ? modsecCrsConfig.getTestRulesFilePath() : modsecCrsConfig.getRulesFilePath(),
        getModsecRuleInfos(ruleVersion, useTestRules),
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
    ModsecCrsConfig crsConfig =
        ModsecCrsConfig.ruleVersionToConfigMap.get(getTestStrippedVersion(ruleVersion));
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
        configConverter.getAnomalyRuleInfos(
            MODSEC_RULE_DETAILS_FILE_PATH, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC);

    Map<String, AnomalyRuleInfo> allMergedModsecRules = new HashMap<>(anomalyRulesInfoMap);
    versionedModsecRules = new HashMap<>();
    versionedTestModsecRules = new HashMap<>();

    for (ModsecRuleVersion ruleVersion : ModsecRuleVersion.values()) {
      if (ruleVersion.equals(ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED)
          || ruleVersion.equals(ModsecRuleVersion.UNRECOGNIZED)
          || isModsecTestRuleVersion(ruleVersion)) {
        continue;
      }
      if (!ModsecCrsConfig.ruleVersionToConfigMap.containsKey(ruleVersion)) {
        throw new IllegalArgumentException(
            String.format("Cannot get modsec rule files for: %s", ruleVersion));
      } else {
        ModsecCrsConfig crsConfig = getModsecCrsConfig(ruleVersion);
        Map<String, List<AnomalySubRuleInfo>> modsecRulesMap;
        {
          modsecRulesMap =
              modsecCrsRulesHandler.parseModsecCrsRules(
                  modsecCrsRulesHandler.loadModsecCrsFileContents(crsConfig.getRulesFilePath()));
          versionedModsecRules.put(
              ruleVersion, mergeAnomalyRuleInfos(anomalyRulesInfoMap, modsecRulesMap, false));
          allMergedModsecRules = mergeAnomalyRuleInfos(allMergedModsecRules, modsecRulesMap, true);
        }
        { // test rules
          modsecRulesMap =
              modsecCrsRulesHandler.parseModsecCrsRules(
                  modsecCrsRulesHandler.loadModsecCrsFileContents(
                      crsConfig.getTestRulesFilePath()));
          versionedTestModsecRules.put(
              ruleVersion, mergeAnomalyRuleInfos(anomalyRulesInfoMap, modsecRulesMap, false));
          allMergedModsecRules = mergeAnomalyRuleInfos(allMergedModsecRules, modsecRulesMap, true);
        }
      }
    }
    versionedModsecRules.put(
        ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED, allMergedModsecRules);
    versionedModsecRules.put(ModsecRuleVersion.UNRECOGNIZED, allMergedModsecRules);
  }

  private Map<String, AnomalyRuleInfo> mergeAnomalyRuleInfos(
      Map<String, AnomalyRuleInfo> defaultAnomalyRulesInfoMap,
      Map<String, List<AnomalySubRuleInfo>> modsecRulesMap,
      boolean keepAllDefault) {

    Map<String, AnomalySubRuleInfo> subRulesMap =
        defaultAnomalyRulesInfoMap.values().stream()
            .map(AnomalyRuleInfo::getSubRuleInfosList)
            .flatMap(List::stream)
            .collect(Collectors.toMap(AnomalySubRuleInfo::getRuleId, Function.identity()));

    Map<String, AnomalyRuleInfo> resultMap =
        keepAllDefault ? new HashMap<>(defaultAnomalyRulesInfoMap) : new HashMap<>();

    modsecRulesMap.entrySet().stream()
        .filter(entry -> defaultAnomalyRulesInfoMap.containsKey(entry.getKey()))
        .forEach(
            entry -> {
              AnomalyRuleInfo.Builder builder =
                  defaultAnomalyRulesInfoMap.get(entry.getKey()).toBuilder();
              Map<String, AnomalySubRuleInfo> subRules = new HashMap<>();
              if (keepAllDefault) {
                builder
                    .getSubRuleInfosList()
                    .forEach(subRule -> subRules.put(subRule.getRuleId(), subRule));
              }
              builder.clearSubRuleInfos();
              entry.getValue().stream()
                  .filter(subRule -> subRulesMap.containsKey(subRule.getRuleId()))
                  .forEach(
                      subRule ->
                          subRules.put(
                              subRule.getRuleId(),
                              mergeSubRules(subRule, subRulesMap.get(subRule.getRuleId()))));
              resultMap.put(
                  builder.getRuleId(), builder.addAllSubRuleInfos(subRules.values()).build());
            });

    return resultMap;
  }

  private AnomalySubRuleInfo mergeSubRules(
      AnomalySubRuleInfo subRuleInfo1, AnomalySubRuleInfo subRuleInfo2) {
    AnomalySubRuleInfo.Builder builder = subRuleInfo1.toBuilder().mergeFrom(subRuleInfo2);
    List<AnomalySubRuleType> subRuleTypes =
        builder.getSubRuleTypesList().stream().distinct().collect(Collectors.toList());
    builder.clearSubRuleTypes().addAllSubRuleTypes(subRuleTypes);
    return builder.build();
  }
}
