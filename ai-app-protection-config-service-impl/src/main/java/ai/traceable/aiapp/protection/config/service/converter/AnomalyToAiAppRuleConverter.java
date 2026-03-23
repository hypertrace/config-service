package ai.traceable.aiapp.protection.config.service.converter;

import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.THREAT_TYPE_ID_LABEL_KEY;

import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiAppOotbRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppSubRule;
import ai.traceable.aiapp.protection.config.service.v1.EventDetails;
import ai.traceable.aiapp.protection.config.service.v1.RuleAction;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatus;
import ai.traceable.aiapp.protection.config.service.v1.SeverityLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.CodeDetectedInPromptAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CodeDetectedInPromptThreatRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converter class that transforms anomaly detection configurations into AI app protection rules.
 * This converter takes anomaly rule information, scoped anomaly detection configurations, and
 * custom rules to produce a comprehensive list of AI app rules.
 */
public final class AnomalyToAiAppRuleConverter {

  private final Logger logger = LoggerFactory.getLogger(AnomalyToAiAppRuleConverter.class);

  private final FeatureCachingClient featureCachingClient;

  @Inject
  AnomalyToAiAppRuleConverter(FeatureCachingClient featureCachingClient) {
    this.featureCachingClient = featureCachingClient;
  }

  /**
   * Converts anomaly detection configurations to a list of AI app rules.
   *
   * @param requestContext Request context for feature flag lookups
   * @param configScope Current anomaly config scope for determining overrides
   * @param scopedAnomalyDetectionConfig Scoped anomaly detection configuration containing GenAI
   *     configs
   * @param anomalyRuleInfos List of anomaly rule information containing basic rule details
   * @param aiAppCustomRules List of converted AI app rules
   * @param unresolvedScopedConfigs List of unresolved scoped anomaly detection configs for
   *     comparison
   */
  public List<AiAppRule> convertToAiAppRules(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      List<AnomalyRuleInfo> anomalyRuleInfos,
      List<AiAppCustomRule> aiAppCustomRules,
      List<ScopedAnomalyDetectionConfig> unresolvedScopedConfigs) {

    if (anomalyRuleInfos == null || anomalyRuleInfos.isEmpty()) {
      logger.warn("No anomaly rule infos provided for conversion");
      return new ArrayList<>();
    }

    logger.debug("Converting {} anomaly rule infos to AI app rules", anomalyRuleInfos.size());

    // Filter anomaly rule infos by event family - only convert GenAI rules to AiAppRule
    List<AnomalyRuleInfo> genAiRuleInfos =
        anomalyRuleInfos.stream()
            .filter(
                ruleInfo ->
                    ruleInfo.getEventFamily() == AnomalyEventFamily.ANOMALY_EVENT_FAMILY_GEN_AI)
            .collect(Collectors.toList());

    // Create maps for ModSec rules (used as sub-rules for codeDetectedInPrompt)
    Map<String, AnomalyRuleInfo> modsecRuleInfosById =
        anomalyRuleInfos.stream()
            .filter(
                ruleInfo ->
                    ruleInfo.getEventFamily() == AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
            .collect(Collectors.toMap(AnomalyRuleInfo::getRuleId, ruleInfo -> ruleInfo));

    logger.debug(
        "Filtered {} GenAI rules and {} ModSec rules from {} total rules",
        genAiRuleInfos.size(),
        modsecRuleInfosById.size(),
        anomalyRuleInfos.size());

    // Create a map of custom rules by threat type ID for efficient lookup
    Map<String, List<AiAppCustomRule>> customRulesByThreatTypeId =
        createCustomRulesMap(aiAppCustomRules);

    // Extract anomaly detection configs for rule status and sub rule actions
    Map<String, AnomalyDetectionConfig> anomalyDetectionConfigsByRuleId =
        extractAnomalyDetectionConfigs(scopedAnomalyDetectionConfig);

    // Pre-compute overriding child scopes for all rules and sub-rules
    Map<String, List<RuleScope>> overridingChildScopesByRuleId =
        computeOverridingChildScopes(
            configScope,
            unresolvedScopedConfigs,
            genAiRuleInfos,
            new ArrayList<>(modsecRuleInfosById.values()));

    // Pre-compute overridden default status for all rules and sub-rules
    Map<String, Boolean> overriddenDefaultByRuleId =
        computeOverriddenDefaults(configScope, unresolvedScopedConfigs);

    // Get hidden rule IDs from feature flags
    Set<String> hiddenRuleIds = featureCachingClient.getHiddenDefenseAiFeatures(requestContext);
    logger.debug(
        "Retrieved {} hidden rule IDs from feature flags: {}", hiddenRuleIds.size(), hiddenRuleIds);

    List<AiAppRule> aiAppRules = new ArrayList<>();

    for (AnomalyRuleInfo anomalyRuleInfo : genAiRuleInfos) {
      try {
        AnomalyDetectionConfig anomalyDetectionConfig =
            anomalyDetectionConfigsByRuleId.get(anomalyRuleInfo.getRuleId());

        AiAppRule aiAppRule =
            convertAnomalyRuleInfo(
                anomalyRuleInfo,
                anomalyDetectionConfig,
                customRulesByThreatTypeId.get(anomalyRuleInfo.getRuleId()),
                modsecRuleInfosById,
                overridingChildScopesByRuleId,
                overriddenDefaultByRuleId,
                hiddenRuleIds);

        aiAppRules.add(aiAppRule);
        logger.debug("Successfully converted anomaly rule: {}", anomalyRuleInfo.getRuleId());
      } catch (Exception e) {
        logger.error("Failed to convert anomaly rule: {}", anomalyRuleInfo.getRuleId(), e);
      }
    }
    return aiAppRules;
  }

  /** Creates a map of custom rules grouped by threat type ID for efficient lookup. */
  private Map<String, List<AiAppCustomRule>> createCustomRulesMap(
      List<AiAppCustomRule> aiAppCustomRules) {
    if (aiAppCustomRules == null || aiAppCustomRules.isEmpty()) {
      return new HashMap<>();
    }

    return aiAppCustomRules.stream()
        .filter(
            customRule ->
                customRule.getRuleData().getEventLabelsMap().containsKey(THREAT_TYPE_ID_LABEL_KEY))
        .collect(
            Collectors.groupingBy(
                customRule ->
                    customRule.getRuleData().getEventLabelsMap().get(THREAT_TYPE_ID_LABEL_KEY)));
  }

  /** Extracts anomaly detection configurations by rule ID. */
  private Map<String, AnomalyDetectionConfig> extractAnomalyDetectionConfigs(
      ScopedAnomalyDetectionConfig scopedConfig) {

    Map<String, AnomalyDetectionConfig> anomalyDetectionConfigs = new HashMap<>();

    if (scopedConfig == null || scopedConfig.getAnomalyDetectionConfigsList().isEmpty()) {
      return anomalyDetectionConfigs;
    }

    for (AnomalyDetectionConfig detectionConfig : scopedConfig.getAnomalyDetectionConfigsList()) {
      if (detectionConfig.hasGenAiAnomalyDetectionConfig()) {
        GenAiAnomalyDetectionConfig genAiConfig = detectionConfig.getGenAiAnomalyDetectionConfig();
        anomalyDetectionConfigs.put(genAiConfig.getAnomalyRuleId(), detectionConfig);
      }
    }

    return anomalyDetectionConfigs;
  }

  /**
   * Pre-computes overriding child scopes for all rules and sub-rules to optimize performance.
   * Creates a map with ruleId/subRuleId as keys and their corresponding overriding child scopes as
   * values.
   */
  private Map<String, List<RuleScope>> computeOverridingChildScopes(
      AnomalyConfigScope configScope,
      List<ScopedAnomalyDetectionConfig> unresolvedScopedConfigs,
      List<AnomalyRuleInfo> genAiAnomalyRuleInfos,
      List<AnomalyRuleInfo> modsecAnomalyRuleInfos) {

    Map<String, List<RuleScope>> overridingChildScopesByRuleId = new HashMap<>();

    // Only compute child scopes if the current scope is a customer scope
    if (configScope == null || !configScope.hasCustomerScope()) {
      return overridingChildScopesByRuleId;
    }

    if (unresolvedScopedConfigs == null || unresolvedScopedConfigs.isEmpty()) {
      return overridingChildScopesByRuleId;
    }

    // Process each GenAI rule to compute main rule overriding child scopes
    for (AnomalyRuleInfo ruleInfo : genAiAnomalyRuleInfos) {
      String ruleId = ruleInfo.getRuleId();

      List<RuleScope> ruleOverridingChildScopes =
          computeRuleOverridingChildScopes(ruleId, unresolvedScopedConfigs);

      if (!ruleOverridingChildScopes.isEmpty()) {
        overridingChildScopesByRuleId.put(ruleId, ruleOverridingChildScopes);
      }

      for (AnomalySubRuleInfo subRuleInfo : ruleInfo.getSubRuleInfosList()) {
        String subRuleId = subRuleInfo.getRuleId();
        List<RuleScope> subRuleOverridingChildScopes =
            computeSubRuleOverridingChildScopes(subRuleId, unresolvedScopedConfigs);

        if (!subRuleOverridingChildScopes.isEmpty()) {
          overridingChildScopesByRuleId.put(subRuleId, subRuleOverridingChildScopes);
        }
      }
    }

    for (AnomalyRuleInfo modsecRuleInfo : modsecAnomalyRuleInfos) {
      String subRuleId = modsecRuleInfo.getRuleId();
      List<RuleScope> subRuleOverridingChildScopes =
          computeSubRuleOverridingChildScopes(subRuleId, unresolvedScopedConfigs);

      if (!subRuleOverridingChildScopes.isEmpty()) {
        overridingChildScopesByRuleId.put(subRuleId, subRuleOverridingChildScopes);
      }
    }

    logger.debug(
        "Pre-computed overriding child scopes for {} rules/sub-rules",
        overridingChildScopesByRuleId.size());
    return overridingChildScopesByRuleId;
  }

  /**
   * Computes overriding child scopes for a main rule by comparing customer-level and
   * environment-level configs.
   */
  private List<RuleScope> computeRuleOverridingChildScopes(
      String ruleId, List<ScopedAnomalyDetectionConfig> unresolvedScopedConfigs) {

    List<RuleScope> overridingChildScopes = new ArrayList<>();

    // Compare with environment-level configs to find overrides
    for (ScopedAnomalyDetectionConfig scopedConfig : unresolvedScopedConfigs) {
      if (scopedConfig.getConfigScope().hasEnvironmentScope()
          && !scopedConfig.getAnomalyDetectionConfigsList().isEmpty()) {

        // Process each anomaly detection config in the scoped config
        for (AnomalyDetectionConfig envConfig : scopedConfig.getAnomalyDetectionConfigsList()) {
          // Check if this environment config is for the same rule
          if (envConfig.hasGenAiAnomalyDetectionConfig()
              && ruleId.equals(envConfig.getGenAiAnomalyDetectionConfig().getAnomalyRuleId())
              && envConfig.getConfigStatus().hasDisabled()) {
            AnomalyEnvironmentScope envScope = scopedConfig.getConfigScope().getEnvironmentScope();
            RuleScope ruleScope =
                RuleScope.newBuilder()
                    .setEnvironmentScope(
                        ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope
                            .newBuilder()
                            .addEnvironmentIds(envScope.getEnvironmentId())
                            .build())
                    .build();
            overridingChildScopes.add(ruleScope);
            break; // Only add once per scoped config
          }
        }
      }
    }

    return overridingChildScopes;
  }

  /**
   * Computes overriding child scopes for a sub-rule by comparing customer-level and
   * environment-level configs.
   */
  private List<RuleScope> computeSubRuleOverridingChildScopes(
      String subRuleId, List<ScopedAnomalyDetectionConfig> unresolvedScopedConfigs) {

    List<RuleScope> overridingChildScopes = new ArrayList<>();

    // Compare with environment-level configs to find overrides
    for (ScopedAnomalyDetectionConfig scopedConfig : unresolvedScopedConfigs) {
      if (scopedConfig.getConfigScope().hasEnvironmentScope()
          && !scopedConfig.getAnomalyDetectionConfigsList().isEmpty()) {

        // Process each anomaly detection config in the scoped config
        for (AnomalyDetectionConfig envConfig : scopedConfig.getAnomalyDetectionConfigsList()) {

          if (envConfig.hasGenAiAnomalyDetectionConfig()) {
            GenAiAnomalyDetectionConfig envGenAiConfig = envConfig.getGenAiAnomalyDetectionConfig();
            if (envGenAiConfig.hasSubRuleConfigs()) {
              Map<String, AnomalySubRuleConfig> envSubRuleConfigMap =
                  envGenAiConfig.getSubRuleConfigs().getSubRuleConfigsMap();
              AnomalySubRuleConfig envSubRuleConfig = envSubRuleConfigMap.get(subRuleId);
              if (envSubRuleConfig != null
                  && (!envSubRuleConfig
                          .getAnomalyRuleAction()
                          .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_UNSPECIFIED)
                      || envSubRuleConfig.getConfigStatus().hasDisabled())) {
                AnomalyEnvironmentScope envScope =
                    scopedConfig.getConfigScope().getEnvironmentScope();
                RuleScope ruleScope =
                    RuleScope.newBuilder()
                        .setEnvironmentScope(
                            ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope
                                .newBuilder()
                                .addEnvironmentIds(envScope.getEnvironmentId())
                                .build())
                        .build();
                overridingChildScopes.add(ruleScope);
                break; // Only add once per scoped config
              }
            }
          }
        }
      }
    }

    return overridingChildScopes;
  }

  /**
   * Pre-computes overridden default status for all rules and sub-rules by checking unresolved
   * configs. A rule/sub-rule is considered overridden if there's a config in
   * unresolvedScopedConfigs for the current scope.
   */
  private Map<String, Boolean> computeOverriddenDefaults(
      AnomalyConfigScope configScope, List<ScopedAnomalyDetectionConfig> unresolvedScopedConfigs) {

    Map<String, Boolean> overriddenDefaultByRuleId = new HashMap<>();

    if (unresolvedScopedConfigs == null || unresolvedScopedConfigs.isEmpty()) {
      return overriddenDefaultByRuleId;
    }
    Optional<ScopedAnomalyDetectionConfig> unresolvedConfig =
        unresolvedScopedConfigs.stream()
            .filter(scopedConfig -> configScope.equals(scopedConfig.getConfigScope()))
            .findFirst();

    if (unresolvedConfig.isPresent()) {
      // Process each anomaly detection config in the unresolved scoped config
      for (AnomalyDetectionConfig detectionConfig :
          unresolvedConfig.get().getAnomalyDetectionConfigsList()) {
        if (detectionConfig.hasGenAiAnomalyDetectionConfig()) {
          GenAiAnomalyDetectionConfig genAiConfig =
              detectionConfig.getGenAiAnomalyDetectionConfig();
          String ruleId = genAiConfig.getAnomalyRuleId();

          // Mark main rule as overridden
          overriddenDefaultByRuleId.put(ruleId, detectionConfig.getConfigStatus().hasDisabled());

          // Mark sub-rules as overridden if they have configs
          if (genAiConfig.hasSubRuleConfigs()) {
            Map<String, AnomalySubRuleConfig> subRuleConfigMap =
                genAiConfig.getSubRuleConfigs().getSubRuleConfigsMap();
            for (String subRuleId : subRuleConfigMap.keySet()) {
              AnomalySubRuleConfig subRuleConfig = subRuleConfigMap.get(subRuleId);
              overriddenDefaultByRuleId.put(
                  subRuleId,
                  !subRuleConfig
                          .getAnomalyRuleAction()
                          .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_UNSPECIFIED)
                      || subRuleConfig.getConfigStatus().hasDisabled());
            }
          }
        }
      }
    }

    logger.debug(
        "Pre-computed overridden default status for {} rules/sub-rules",
        overriddenDefaultByRuleId.size());
    return overriddenDefaultByRuleId;
  }

  /** Converts anomaly rule info to an AI app rule. */
  private AiAppRule convertAnomalyRuleInfo(
      AnomalyRuleInfo anomalyRuleInfo,
      AnomalyDetectionConfig anomalyDetectionConfig,
      List<AiAppCustomRule> matchingCustomRules,
      Map<String, AnomalyRuleInfo> modsecRuleInfosById,
      Map<String, List<RuleScope>> overridingChildScopesByRuleId,
      Map<String, Boolean> overriddenDefaultByRuleId,
      Set<String> hiddenRuleIds) {

    // Determine override information for the main rule
    boolean ruleOverriddenDefault =
        overriddenDefaultByRuleId.getOrDefault(anomalyRuleInfo.getRuleId(), false);
    List<RuleScope> ruleOverridingChildScopes =
        overridingChildScopesByRuleId.getOrDefault(anomalyRuleInfo.getRuleId(), new ArrayList<>());

    // Determine if the main rule should be hidden
    boolean ruleHidden = hiddenRuleIds.contains(anomalyRuleInfo.getRuleId());
    logger.debug("Rule {} hidden status: {}", anomalyRuleInfo.getRuleId(), ruleHidden);

    AiAppRule.Builder aiAppRuleBuilder =
        AiAppRule.newBuilder()
            .setRuleId(anomalyRuleInfo.getRuleId())
            .setRuleName(anomalyRuleInfo.getRuleName())
            .putAllEventLabels(anomalyRuleInfo.getEventLabelsMap())
            .setRuleStatus(convertRuleStatus(anomalyDetectionConfig))
            .setEventDetails(convertEventDetails(anomalyRuleInfo.getEventDetails()))
            .setOverriddenDefault(ruleOverriddenDefault)
            .addAllOverridingChildScopes(ruleOverridingChildScopes)
            .setHidden(ruleHidden);

    List<AiAppSubRule> allSubRules = new ArrayList<>();

    // Get sub rule config map for use in both anomaly sub rules and ModSec rules
    GenAiAnomalyDetectionConfig genAiConfig =
        anomalyDetectionConfig != null && anomalyDetectionConfig.hasGenAiAnomalyDetectionConfig()
            ? anomalyDetectionConfig.getGenAiAnomalyDetectionConfig()
            : null;
    Map<String, AnomalySubRuleConfig> subRuleConfigMap = getSubRuleConfigMap(genAiConfig);

    // Convert anomaly sub rule infos to OOTB sub rules using sub rule configs
    if (!anomalyRuleInfo.getSubRuleInfosList().isEmpty()) {
      List<AiAppSubRule> ootbSubRules =
          anomalyRuleInfo.getSubRuleInfosList().stream()
              .map(
                  subRuleInfo ->
                      convertAnomalySubRuleInfoToSubRule(
                          subRuleInfo,
                          subRuleConfigMap.get(subRuleInfo.getRuleId()),
                          overridingChildScopesByRuleId,
                          overriddenDefaultByRuleId,
                          hiddenRuleIds))
              .collect(Collectors.toList());

      allSubRules.addAll(ootbSubRules);
    }

    // Add ModSec rules as sub-rules for codeDetectedInPrompt rule
    if (genAiConfig != null && genAiConfig.hasCodeDetectedInPrompt()) {
      List<AiAppSubRule> modsecSubRules =
          convertModSecRulesToSubRules(
              genAiConfig.getCodeDetectedInPrompt(),
              modsecRuleInfosById,
              subRuleConfigMap,
              overridingChildScopesByRuleId,
              overriddenDefaultByRuleId,
              hiddenRuleIds);
      allSubRules.addAll(modsecSubRules);
      logger.debug(
          "Added {} ModSec sub-rules to codeDetectedInPrompt rule: {}",
          modsecSubRules.size(),
          anomalyRuleInfo.getRuleId());
    }

    // Add custom rules as sub rules
    if (matchingCustomRules != null && !matchingCustomRules.isEmpty()) {
      List<AiAppSubRule> customSubRules =
          matchingCustomRules.stream()
              .map(rule -> updateCustomRuleLabels(rule, anomalyRuleInfo.getEventLabelsMap()))
              .map(this::convertCustomRuleToSubRule)
              .collect(Collectors.toList());
      allSubRules.addAll(customSubRules);
    }

    aiAppRuleBuilder.addAllAiAppSubRules(allSubRules);

    return aiAppRuleBuilder.build();
  }

  /** Converts anomaly sub rule info to AI app sub rule using sub rule config. */
  private AiAppSubRule convertAnomalySubRuleInfoToSubRule(
      AnomalySubRuleInfo subRuleInfo,
      AnomalySubRuleConfig subRuleConfig,
      Map<String, List<RuleScope>> overridingChildScopesByRuleId,
      Map<String, Boolean> overriddenDefaultByRuleId,
      Set<String> hiddenRuleIds) {

    AiAppOotbRule.Builder ootbRuleBuilder =
        AiAppOotbRule.newBuilder()
            .setRuleId(subRuleInfo.getRuleId())
            .setRuleName(subRuleInfo.getRuleName())
            .putAllEventLabels(subRuleInfo.getEventLabelsMap())
            .setSeverityLevel(convertAnomalySeverityLevel(subRuleInfo.getSeverityLevel()))
            .setEventDetails(convertEventDetails(subRuleInfo.getEventDetails()));

    // Use sub rule config for rule action and internal flag
    if (subRuleConfig != null) {
      ootbRuleBuilder.setRuleAction(convertAnomalyRuleAction(subRuleConfig.getAnomalyRuleAction()));
      ootbRuleBuilder.setInternal(subRuleConfig.getInternal());
    } else {
      // Default values when no sub rule config is available
      ootbRuleBuilder.setRuleAction(RuleAction.RULE_ACTION_MONITOR);
      ootbRuleBuilder.setInternal(false);
    }

    // Determine override information for the sub-rule using pre-computed map
    boolean subRuleOverriddenDefault =
        overriddenDefaultByRuleId.getOrDefault(subRuleInfo.getRuleId(), false);
    List<RuleScope> subRuleOverridingChildScopes =
        overridingChildScopesByRuleId.getOrDefault(subRuleInfo.getRuleId(), new ArrayList<>());

    // Determine if the sub-rule should be hidden
    boolean subRuleHidden = isSubRuleHidden(subRuleInfo.getRuleId(), hiddenRuleIds);
    logger.debug("Sub-rule {} hidden status: {}", subRuleInfo.getRuleId(), subRuleHidden);

    ootbRuleBuilder.setOverriddenDefault(subRuleOverriddenDefault);
    ootbRuleBuilder.addAllOverridingChildScopes(subRuleOverridingChildScopes);
    ootbRuleBuilder.setHidden(subRuleHidden);

    return AiAppSubRule.newBuilder().setOotbRule(ootbRuleBuilder.build()).build();
  }

  /**
   * Converts ModSec rules to AI app sub-rules based on CodeDetectedInPromptAnomalyDetectionConfig.
   */
  private List<AiAppSubRule> convertModSecRulesToSubRules(
      CodeDetectedInPromptAnomalyDetectionConfig codeDetectedConfig,
      Map<String, AnomalyRuleInfo> modsecRuleInfosById,
      Map<String, AnomalySubRuleConfig> subRuleConfigMap,
      Map<String, List<RuleScope>> overridingChildScopesByRuleId,
      Map<String, Boolean> overriddenDefaultByRuleId,
      Set<String> hiddenRuleIds) {

    List<AiAppSubRule> modsecSubRules = new ArrayList<>();

    if (codeDetectedConfig == null || codeDetectedConfig.getThreatRuleConfigsList().isEmpty()) {
      logger.error("No sub rules found for codeDetectedInPrompt");
      return modsecSubRules;
    }

    for (CodeDetectedInPromptThreatRuleConfig threatRuleConfig :
        codeDetectedConfig.getThreatRuleConfigsList()) {
      String threatRuleId = threatRuleConfig.getThreatRuleId();

      // Find the corresponding ModSec rule
      AnomalyRuleInfo modsecRuleInfo = modsecRuleInfosById.get(threatRuleId);
      if (modsecRuleInfo != null) {
        try {
          // Get sub-rule config for this threat rule
          AnomalySubRuleConfig subRuleConfig = subRuleConfigMap.get(threatRuleId);
          AiAppSubRule modsecSubRule =
              convertModSecRuleInfoToSubRule(
                  modsecRuleInfo,
                  subRuleConfig,
                  overridingChildScopesByRuleId,
                  overriddenDefaultByRuleId,
                  hiddenRuleIds);
          modsecSubRules.add(modsecSubRule);
          logger.debug(
              "Converted ModSec rule {} to sub-rule for codeDetectedInPrompt", threatRuleId);
        } catch (Exception e) {
          logger.error("Failed to convert ModSec rule {} to sub-rule", threatRuleId, e);
        }
      } else {
        logger.warn(
            "ModSec rule {} specified in CodeDetectedInPromptThreatRuleConfig not found",
            threatRuleId);
      }
    }
    return modsecSubRules;
  }

  /** Converts a ModSec AnomalyRuleInfo to an AI app sub-rule using AnomalySubRuleConfig. */
  private AiAppSubRule convertModSecRuleInfoToSubRule(
      AnomalyRuleInfo modsecRuleInfo,
      AnomalySubRuleConfig subRuleConfig,
      Map<String, List<RuleScope>> overridingChildScopesByRuleId,
      Map<String, Boolean> overriddenDefaultByRuleId,
      Set<String> hiddenRuleIds) {
    AiAppOotbRule.Builder ootbRuleBuilder =
        AiAppOotbRule.newBuilder()
            .setRuleId(modsecRuleInfo.getRuleId())
            .setRuleName(modsecRuleInfo.getRuleName())
            .putAllEventLabels(modsecRuleInfo.getEventLabelsMap())
            .setEventDetails(convertEventDetails(modsecRuleInfo.getEventDetails()));

    // Use sub-rule config for severity, rule action, and internal flag if available
    if (subRuleConfig != null) {
      // Use severity from sub-rule config category config
      if (subRuleConfig.hasCategoryConfig()) {
        ootbRuleBuilder.setSeverityLevel(
            convertAnomalyEventScoreCategory(
                subRuleConfig.getCategoryConfig().getEventScoreCategory()));
      } else {
        ootbRuleBuilder.setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_MEDIUM);
      }

      ootbRuleBuilder.setRuleAction(convertAnomalyRuleAction(subRuleConfig.getAnomalyRuleAction()));
      ootbRuleBuilder.setInternal(subRuleConfig.getInternal());
    } else {
      // Default values when no sub-rule config is available
      ootbRuleBuilder.setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_MEDIUM);
      ootbRuleBuilder.setRuleAction(RuleAction.RULE_ACTION_MONITOR);
      ootbRuleBuilder.setInternal(false);
    }

    // Determine override information for the ModSec sub-rule using pre-computed map
    boolean modsecSubRuleOverriddenDefault =
        overriddenDefaultByRuleId.getOrDefault(modsecRuleInfo.getRuleId(), false);
    List<RuleScope> modsecSubRuleOverridingChildScopes =
        overridingChildScopesByRuleId.getOrDefault(modsecRuleInfo.getRuleId(), new ArrayList<>());

    // Determine if the ModSec sub-rule should be hidden
    boolean modsecSubRuleHidden = isSubRuleHidden(modsecRuleInfo.getRuleId(), hiddenRuleIds);
    logger.debug(
        "ModSec sub-rule {} hidden status: {}", modsecRuleInfo.getRuleId(), modsecSubRuleHidden);

    ootbRuleBuilder.setOverriddenDefault(modsecSubRuleOverriddenDefault);
    ootbRuleBuilder.addAllOverridingChildScopes(modsecSubRuleOverridingChildScopes);
    ootbRuleBuilder.setHidden(modsecSubRuleHidden);

    return AiAppSubRule.newBuilder().setOotbRule(ootbRuleBuilder.build()).build();
  }

  private boolean isSubRuleHidden(String subRuleId, Set<String> hiddenRuleIds) {
    return hiddenRuleIds.contains(subRuleId)
        || hiddenRuleIds.stream().anyMatch(subRuleId::startsWith);
  }

  /** Converts anomaly event details to AI app event details. */
  private EventDetails convertEventDetails(
      ai.traceable.anomaly.config.service.v1.AnomalyEventDetails anomalyEventDetails) {

    return EventDetails.newBuilder()
        .setDescription(anomalyEventDetails.getDescription())
        .setMitigation(anomalyEventDetails.getMitigation())
        .setImpact(anomalyEventDetails.getImpact())
        .setReferences(anomalyEventDetails.getReferences())
        .build();
  }

  /**
   * Converts anomaly detection config to rule status using AnomalyConfigStatusChange. Rule status
   * is determined at the main rule level, not from sub rule configs.
   */
  private RuleStatus convertRuleStatus(AnomalyDetectionConfig anomalyDetectionConfig) {
    RuleStatus.Builder statusBuilder = RuleStatus.newBuilder();

    // Use config_status from AnomalyDetectionConfig if available
    if (anomalyDetectionConfig != null && anomalyDetectionConfig.hasConfigStatus()) {
      AnomalyConfigStatusChange configStatus = anomalyDetectionConfig.getConfigStatus();

      // Set disabled flag from config status
      statusBuilder.setDisabled(configStatus.getDisabled());

      // Set internal flag from config status
      statusBuilder.setInternal(configStatus.getInternal());
    } else {
      // Set default values when no config status is available
      statusBuilder.setDisabled(false);
      statusBuilder.setInternal(false);
    }

    return statusBuilder.build();
  }

  /** Extracts sub rule config map from GenAI anomaly detection config. */
  private Map<String, AnomalySubRuleConfig> getSubRuleConfigMap(
      GenAiAnomalyDetectionConfig genAiConfig) {
    if (genAiConfig != null && genAiConfig.hasSubRuleConfigs()) {
      return genAiConfig.getSubRuleConfigs().getSubRuleConfigsMap();
    }
    return new HashMap<>();
  }

  private AiAppCustomRule updateCustomRuleLabels(
      AiAppCustomRule customRule, Map<String, String> labels) {
    AiAppCustomRuleData customRuleData =
        customRule.getRuleData().toBuilder().putAllEventLabels(labels).build();
    return customRule.toBuilder().setRuleData(customRuleData).build();
  }

  /** Converts a custom rule to a sub rule. */
  private AiAppSubRule convertCustomRuleToSubRule(AiAppCustomRule customRule) {
    return AiAppSubRule.newBuilder().setCustomRule(customRule).build();
  }

  /** Converts anomaly severity level to AI app severity level. */
  private SeverityLevel convertAnomalySeverityLevel(
      ai.traceable.anomaly.config.service.v1.AnomalySeverityLevel anomalySeverityLevel) {

    switch (anomalySeverityLevel) {
      case ANOMALY_SEVERITY_LEVEL_LOW:
        return SeverityLevel.SEVERITY_LEVEL_LOW;
      case ANOMALY_SEVERITY_LEVEL_MEDIUM:
        return SeverityLevel.SEVERITY_LEVEL_MEDIUM;
      case ANOMALY_SEVERITY_LEVEL_HIGH:
        return SeverityLevel.SEVERITY_LEVEL_HIGH;
      case ANOMALY_SEVERITY_LEVEL_CRITICAL:
        return SeverityLevel.SEVERITY_LEVEL_CRITICAL;
      case ANOMALY_SEVERITY_LEVEL_UNSPECIFIED:
      default:
        return SeverityLevel.SEVERITY_LEVEL_UNSPECIFIED;
    }
  }

  /** Converts anomaly rule action to AI app rule action. */
  private RuleAction convertAnomalyRuleAction(
      ai.traceable.anomaly.config.service.v1.AnomalyRuleAction anomalyRuleAction) {

    switch (anomalyRuleAction) {
      case ANOMALY_RULE_ACTION_DISABLE:
        return RuleAction.RULE_ACTION_DISABLE;
      case ANOMALY_RULE_ACTION_MONITOR:
        return RuleAction.RULE_ACTION_MONITOR;
      case ANOMALY_RULE_ACTION_BLOCK:
        return RuleAction.RULE_ACTION_BLOCK;
      case ANOMALY_RULE_ACTION_TESTING:
        return RuleAction.RULE_ACTION_TESTING;
      case ANOMALY_RULE_ACTION_UNSPECIFIED:
      default:
        return RuleAction.RULE_ACTION_UNSPECIFIED;
    }
  }

  /** Converts AnomalyEventScoreCategory to AI app SeverityLevel. */
  private SeverityLevel convertAnomalyEventScoreCategory(
      ai.traceable.anomaly.config.service.v1.detector.AnomalyEventScoreCategory
          eventScoreCategory) {
    switch (eventScoreCategory) {
      case ANOMALY_EVENT_SCORE_CATEGORY_LOW:
        return SeverityLevel.SEVERITY_LEVEL_LOW;
      case ANOMALY_EVENT_SCORE_CATEGORY_MEDIUM:
        return SeverityLevel.SEVERITY_LEVEL_MEDIUM;
      case ANOMALY_EVENT_SCORE_CATEGORY_HIGH:
        return SeverityLevel.SEVERITY_LEVEL_HIGH;
      case ANOMALY_EVENT_SCORE_CATEGORY_CRITICAL:
        return SeverityLevel.SEVERITY_LEVEL_CRITICAL;
      default:
        return SeverityLevel.SEVERITY_LEVEL_UNSPECIFIED;
    }
  }
}
