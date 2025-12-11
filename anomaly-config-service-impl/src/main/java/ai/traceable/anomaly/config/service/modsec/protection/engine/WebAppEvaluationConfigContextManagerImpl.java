package ai.traceable.anomaly.config.service.modsec.protection.engine;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils.ANOMALY_CONFIG_SCOPE_COMPARATOR;
import static ai.traceable.anomaly.config.service.modsec.Utils.getModsecAnomalyRuleConfigMap;
import static ai.traceable.anomaly.config.service.modsec.Utils.getSubRuleConfigMap;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigManager;
import ai.traceable.anomaly.config.service.global.ruleinfo.WebAppRuleInfoProvider;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.modsec.ModsecConfigServiceConfig;
import ai.traceable.anomaly.config.service.modsec.rules.ModsecManager;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.RuleEvaluationPoint;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionData;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.GlobalModsecConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.modsec.GetWebAppEvaluationConfigContextRequest;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.entity.fetcher.cache.CachedApiMappingProvider;
import ai.traceable.entity.fetcher.cache.CachedServiceMappingProvider;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import ai.traceable.protection.engine.config.webapp.v1.SecRuleProcessorConfig;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfig;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationConfigContext;
import ai.traceable.protection.engine.config.webapp.v1.WebAppEvaluationRulesContext;
import ai.traceable.protection.processing.common.v1.ScopeContext;
import ai.traceable.protection.processor.secrules.v1.CorazaEngineVersion;
import ai.traceable.protection.processor.secrules.v1.CorazaRuleDirectivesType;
import ai.traceable.protection.processor.secrules.v1.CorazaRuleProcessor;
import ai.traceable.protection.processor.secrules.v1.ModsecJniRuleDirectivesType;
import ai.traceable.protection.processor.secrules.v1.ModsecJniRuleProcessor;
import ai.traceable.protection.processor.secrules.v1.SecRuleProcessorDetails;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.change.event.v1.ConfigChangeEventKey;
import org.hypertrace.config.change.event.v1.ConfigChangeEventValue;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.kafka.event.listener.KafkaLiveEventListener;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Singleton
@Slf4j
public class WebAppEvaluationConfigContextManagerImpl
    implements WebAppEvaluationConfigContextManager {
  private static final String CACHE_NAME = "webAppConfigContextCache";
  private static final GetAnomalyDetectionConfigsFilter ANOMALY_DETECTION_CONFIGS_FILTER =
      GetAnomalyDetectionConfigsFilter.newBuilder()
          .addAnomalyDetectionConfigTypes(
              AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY)
          .build();
  private static final List<AnomalySubRuleType> ALL_SUB_RULE_TYPES =
      List.of(
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
          AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR);

  private static final String EMPTY_STRING = "";
  private final ModsecManager modsecManager;
  private final ModsecRulesRegistry modsecRulesRegistry;
  private final AnomalyDetectionConfigManager anomalyDetectionConfigManager;
  private final GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;
  private final FeatureCachingClient featureCachingClient;
  private final WebAppRuleInfoProvider webAppRuleInfoProvider;
  private final ModsecRuleVersion defaultModsecRuleVersion;
  private final CachedServiceMappingProvider cachedServiceMappingProvider;
  private final CachedApiMappingProvider cachedApiMappingProvider;
  private final LoadingCache<ContextualKey<RequestData>, WebAppEvaluationConfigContext>
      webAppConfigContextCache;

  @Inject
  public WebAppEvaluationConfigContextManagerImpl(
      ModsecManager modsecManager,
      ModsecRulesRegistry modsecRulesRegistry,
      AnomalyDetectionConfigManager anomalyDetectionConfigManager,
      GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager,
      FeatureCachingClient featureCachingClient,
      WebAppRuleInfoProvider webAppRuleInfoProvider,
      ModsecRuleVersion defaultModsecRuleVersion,
      CachedServiceMappingProvider cachedServiceMappingProvider,
      CachedApiMappingProvider cachedApiMappingProvider,
      ModsecConfigServiceConfig modsecConfigServiceConfig,
      KafkaLiveEventListener<ConfigChangeEventKey, ConfigChangeEventValue> kafkaLiveEventListener) {
    this.modsecManager = modsecManager;
    this.modsecRulesRegistry = modsecRulesRegistry;
    this.anomalyDetectionConfigManager = anomalyDetectionConfigManager;
    this.globalAnomalyConfigStatusManager = globalAnomalyConfigStatusManager;
    this.featureCachingClient = featureCachingClient;
    this.webAppRuleInfoProvider = webAppRuleInfoProvider;
    this.defaultModsecRuleVersion = defaultModsecRuleVersion;
    this.cachedServiceMappingProvider = cachedServiceMappingProvider;
    this.cachedApiMappingProvider = cachedApiMappingProvider;
    this.webAppConfigContextCache = buildCache(modsecConfigServiceConfig);
    registerCacheMetrics(modsecConfigServiceConfig);
    try {
      kafkaLiveEventListener.registerCallback(this::updateCacheBasedOnEvent);
    } catch (Exception e) {
      log.warn(
          "Failed to register Kafka event listener for {}, cache invalidation will not work",
          CACHE_NAME,
          e);
    }
  }

  @Override
  public WebAppEvaluationConfigContext getWebAppEvaluationConfigContext(
      RequestContext requestContext, GetWebAppEvaluationConfigContextRequest request) {
    if (!featureCachingClient.isProtectionEngineWebAppProtectionEnabledForTenant(requestContext)) {
      log.debug(
          "Protection engine web app protection is disabled for tenant: {}",
          requestContext.getTenantId());
      return WebAppEvaluationConfigContext.getDefaultInstance();
    }
    RuleEvaluationPoint ruleEvaluationPoint = request.getRuleEvaluationPoint();
    switch (ruleEvaluationPoint) {
      case RULE_EVALUATION_POINT_EDGE:
        try {
          return webAppConfigContextCache.get(
              requestContext.buildInternalContextualKey(RequestData.from(request)));
        } catch (ExecutionException e) {
          log.error(
              "Error loading from cache for tenant: {}, falling back to direct computation",
              requestContext.getTenantId(),
              e);
          return WebAppEvaluationConfigContext.getDefaultInstance();
        }
      case RULE_EVALUATION_POINT_PLATFORM:
        return computeWebAppEvaluationConfigContext(requestContext, request);
      default:
        log.warn(
            "Unknown RuleEvaluationPoint: {}. Returning empty config context for requestContext: {}",
            ruleEvaluationPoint,
            requestContext);
        return WebAppEvaluationConfigContext.getDefaultInstance();
    }
  }

  private WebAppEvaluationConfigContext computeWebAppEvaluationConfigContext(
      RequestContext requestContext, GetWebAppEvaluationConfigContextRequest request) {
    log.debug(
        "Starting getWebAppEvaluationConfigContext for tenant: {}, with request: ruleEvaluationPoint={}, subRuleTypes={}",
        requestContext.getTenantId(),
        request.getRuleEvaluationPoint(),
        request.getSubRuleTypesList());

    Map<AnomalyConfigScope, ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigMap =
        getScopedAnomalyDetectionConfigMap(requestContext);
    log.debug(
        "Retrieved scopedAnomalyDetectionConfigMap with {} entries",
        scopedAnomalyDetectionConfigMap.size());

    Map<AnomalyConfigScope, ScopedAnomalyConfigStatus> scopedAnomalyConfigStatusMap =
        getScopedAnomalyConfigStatusMap(requestContext);
    log.debug(
        "Retrieved scopedAnomalyConfigStatusMap with {} entries",
        scopedAnomalyConfigStatusMap.size());

    TreeSet<AnomalyConfigScope> configScopes = new TreeSet<>(ANOMALY_CONFIG_SCOPE_COMPARATOR);
    configScopes.addAll(scopedAnomalyDetectionConfigMap.keySet());
    configScopes.addAll(scopedAnomalyConfigStatusMap.keySet());
    log.debug("Combined config scopes: {} total scopes", configScopes.size());
    log.debug("Sorted config scopes by priority: {}", configScopes);

    List<WebAppEvaluationConfig> webAppEvaluationConfigs = new ArrayList<>();
    List<SecRuleProcessorConfig> secRuleProcessorConfigs = new ArrayList<>();
    List<AnomalyConfigScope> configScopesWithEmptyBlob = new ArrayList<>();
    Map<AnomalyConfigScope, ScopeContext> scopeContextMap =
        AnomalyConfigScopeUtils.getScopeContextMap(
            requestContext, configScopes, cachedApiMappingProvider, cachedServiceMappingProvider);
    log.debug("Retrieved scope context map with {} entries", scopeContextMap.size());
    Map<RulesContextArgs, List<AnomalyConfigScope>> rulesContextArgsToScopesMap = new HashMap<>();

    for (AnomalyConfigScope configScope : configScopes) {
      log.debug("Processing config scope: {}", configScope.getScopeCase());
      List<AnomalyConfigScope> scopesWithDecreasingPriority =
          AnomalyConfigScopeUtils.getConfigScopesWithDecreasingPriority(configScope);

      ScopedAnomalyConfigStatus scopedAnomalyConfigStatus =
          scopesWithDecreasingPriority.stream()
              .filter(scopedAnomalyConfigStatusMap::containsKey)
              .findFirst()
              .map(scopedAnomalyConfigStatusMap::get)
              .orElse(ScopedAnomalyConfigStatus.getDefaultInstance());

      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig =
          scopesWithDecreasingPriority.stream()
              .filter(scopedAnomalyDetectionConfigMap::containsKey)
              .findFirst()
              .map(scopedAnomalyDetectionConfigMap::get)
              .orElse(ScopedAnomalyDetectionConfig.getDefaultInstance());
      ModsecRuleVersion modsecRuleVersion = getModsecRuleVersion(scopedAnomalyDetectionConfig);
      log.debug("Using modsec rule version: {}", modsecRuleVersion);

      RuleVersionData ruleVersionData =
          scopedAnomalyConfigStatus.getGlobalModsecConfig().getRuleVersionData();
      RuleVersion ruleVersion = getRuleVersion(ruleVersionData, requestContext);
      log.debug("Using rule version: {}", ruleVersion);

      List<AnomalySubRuleType> anomalySubRuleTypes =
          getAnomalySubRuleTypes(
              scopedAnomalyConfigStatus,
              request.getRuleEvaluationPoint(),
              request.getSubRuleTypesList());
      log.debug("Filtered anomaly sub rule types: {}", anomalySubRuleTypes);

      // No sub-rules - this can only happen if only REGULAR rules are requested for
      // RULE_EVALUATION_POINT_EDGE & as per
      // scopedAnomalyConfigStatus - blocking is disabled for regular rules
      // In this case we should send an empty blob because we need to relay this information to the
      // client so that if a span/request matching this scope comes in, we can correctly perform
      // scope matching and just perform no-op blocking evaluations for it.
      if (anomalySubRuleTypes.isEmpty()) {
        log.debug("No anomaly sub rule types found, adding empty WebAppEvaluationRulesContext");
        configScopesWithEmptyBlob.add(configScope);
        continue;
      }
      if (!scopedAnomalyConfigStatus.equals(ScopedAnomalyConfigStatus.getDefaultInstance())) {
        log.debug("Adding SecRuleProcessorConfig for non-default scopedAnomalyConfigStatus");
        secRuleProcessorConfigs.add(
            getSecRuleProcessorConfig(
                scopedAnomalyConfigStatus, modsecRuleVersion, scopeContextMap.get(configScope)));
      }
      if (!scopedAnomalyDetectionConfig.equals(ScopedAnomalyDetectionConfig.getDefaultInstance())) {
        log.debug("Processing non-default scopedAnomalyDetectionConfig");
        Optional<WebAppEvaluationConfig> webAppEvaluationConfig =
            getWebAppEvaluationConfig(
                scopedAnomalyDetectionConfig,
                modsecRuleVersion,
                request.getRuleEvaluationPoint(),
                scopeContextMap.get(configScope),
                ruleVersion);

        if (webAppEvaluationConfig.isPresent()) {
          log.debug("Adding WebAppEvaluationConfig");
          webAppEvaluationConfigs.add(webAppEvaluationConfig.get());
        } else {
          log.debug("WebAppEvaluationConfig not present, skipping");
        }
        computeIntermediateStateForWebAppEvaluationRulesContext(
            modsecRuleVersion,
            anomalySubRuleTypes,
            ruleVersion,
            configScope,
            rulesContextArgsToScopesMap);
      }
    }
    Map<String, String> crsBlobSha256ToCrsBlobMap = new HashMap<>();
    List<WebAppEvaluationRulesContext> webAppEvaluationRulesContextList =
        getWebAppEvaluationRulesContextList(
            rulesContextArgsToScopesMap,
            scopeContextMap,
            configScopesWithEmptyBlob,
            crsBlobSha256ToCrsBlobMap);

    WebAppEvaluationConfigContext result =
        WebAppEvaluationConfigContext.newBuilder()
            .addAllEvaluationConfigs(webAppEvaluationConfigs)
            .addAllSecRuleProcessorConfigs(secRuleProcessorConfigs)
            .addAllWebAppEvaluationRulesContexts(webAppEvaluationRulesContextList)
            .putAllCrsRulesBlobIdToBlob(crsBlobSha256ToCrsBlobMap)
            .build();

    log.debug(
        "Completed getWebAppEvaluationConfigContext: {} evaluation configs, {} sec rule processor configs, {} rules contexts",
        webAppEvaluationConfigs.size(),
        secRuleProcessorConfigs.size(),
        webAppEvaluationRulesContextList.size());

    return result;
  }

  private List<WebAppEvaluationRulesContext> getWebAppEvaluationRulesContextList(
      Map<RulesContextArgs, List<AnomalyConfigScope>> rulesContextArgsToScopesMap,
      Map<AnomalyConfigScope, ScopeContext> scopeContextMap,
      List<AnomalyConfigScope> configScopesWithEmptyBlob,
      Map<String, String> crsBlobSha256ToCrsBlobMap) {
    if (log.isDebugEnabled()) {
      int totalScopes = rulesContextArgsToScopesMap.values().stream().mapToInt(List::size).sum();
      int totalRulesContextArgs = rulesContextArgsToScopesMap.size();
      log.debug(
          "getWebAppEvaluationRulesContextList called with totalScopes: {}, totalRulesContextArgs: {}, configScopesWithEmptyBlob: {}",
          totalScopes,
          totalRulesContextArgs,
          configScopesWithEmptyBlob);
    }
    TreeMap<AnomalyConfigScope, String> scopeToCrsBlobSha256Map =
        new TreeMap<>(ANOMALY_CONFIG_SCOPE_COMPARATOR);
    for (Map.Entry<RulesContextArgs, List<AnomalyConfigScope>> entry :
        rulesContextArgsToScopesMap.entrySet()) {
      RulesContextArgs rulesContextArgs = entry.getKey();
      List<AnomalyConfigScope> anomalyConfigScopes = entry.getValue();
      String crsBlob =
          getCrsBlob(
              rulesContextArgs.getModsecRuleVersion(),
              rulesContextArgs.getAnomalySubRuleTypes(),
              rulesContextArgs.getRuleVersion());
      String crsBlobSha256 = HashUtil.calculateSHA256(crsBlob);
      crsBlobSha256ToCrsBlobMap.put(crsBlobSha256, crsBlob);
      anomalyConfigScopes.forEach(scope -> scopeToCrsBlobSha256Map.put(scope, crsBlobSha256));
    }
    if (!configScopesWithEmptyBlob.isEmpty()) {
      String sha256ForEmptyBlob = HashUtil.calculateSHA256(EMPTY_STRING);
      configScopesWithEmptyBlob.forEach(
          scope -> scopeToCrsBlobSha256Map.put(scope, sha256ForEmptyBlob));
      crsBlobSha256ToCrsBlobMap.put(sha256ForEmptyBlob, EMPTY_STRING);
    }
    List<WebAppEvaluationRulesContext> webAppEvaluationRulesContextList = new ArrayList<>();
    for (Map.Entry<AnomalyConfigScope, String> entry : scopeToCrsBlobSha256Map.entrySet()) {
      webAppEvaluationRulesContextList.add(
          WebAppEvaluationRulesContext.newBuilder()
              .setScopeContext(scopeContextMap.get(entry.getKey()))
              .setCrsRulesBlobId(entry.getValue())
              .build());
    }
    return webAppEvaluationRulesContextList;
  }

  private String getCrsBlob(
      ModsecRuleVersion modsecRuleVersion,
      List<AnomalySubRuleType> anomalySubRuleTypes,
      RuleVersion ruleVersion) {
    log.debug(
        "getCrsBlob called with modsecRuleVersion={}, anomalySubRuleTypes={}, ruleVersion={}",
        modsecRuleVersion,
        anomalySubRuleTypes,
        ruleVersion);
    return modsecManager
        .getModsecCrsRules(anomalySubRuleTypes, modsecRuleVersion, false, ruleVersion, false)
        .getAggregatedModsecBlob();
  }

  private void computeIntermediateStateForWebAppEvaluationRulesContext(
      ModsecRuleVersion modsecRuleVersion,
      List<AnomalySubRuleType> anomalySubRuleTypes,
      RuleVersion ruleVersion,
      AnomalyConfigScope configScope,
      Map<RulesContextArgs, List<AnomalyConfigScope>> rulesContextArgsToScopesMap) {
    // Note: This method computes intermediate state for web app evaluation rules context.
    // rulesContextArgsToScopesMap - is the intermediate state. The purpose of this state is to
    // group the scopes by rules context args. This is used to de-dup the crs-blob related
    // operations performed - fetching crs-blob & computing its sha-256 hash.
    log.debug(
        "computeIntermediateStateForWebAppEvaluationRulesContext called for configScope: {}",
        configScope);
    RulesContextArgs rulesContextArgs =
        new RulesContextArgs(modsecRuleVersion, anomalySubRuleTypes, ruleVersion);
    List<AnomalyConfigScope> anomalyConfigScopes =
        rulesContextArgsToScopesMap.computeIfAbsent(rulesContextArgs, k -> new ArrayList<>());
    anomalyConfigScopes.add(configScope);
  }

  private List<AnomalySubRuleType> getAnomalySubRuleTypes(
      ScopedAnomalyConfigStatus scopedAnomalyConfigStatus,
      RuleEvaluationPoint ruleEvaluationPoint,
      List<AnomalySubRuleType> subRuleTypes) {
    log.debug(
        "getAnomalySubRuleTypes called with ruleEvaluationPoint={}, subRuleTypes={}",
        ruleEvaluationPoint,
        subRuleTypes);

    switch (ruleEvaluationPoint) {
      case RULE_EVALUATION_POINT_EDGE:
        boolean blockingAvailable =
            scopedAnomalyConfigStatus.getGlobalModsecConfig().getBlockingAvailableForRegularRules();
        log.debug("EDGE evaluation point: blockingAvailableForRegularRules={}", blockingAvailable);
        if (!blockingAvailable) {
          return filterSubRuleTypes(
              subRuleTypes,
              List.of(
                  AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE,
                  AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK));
        }
        return filterSubRuleTypes(subRuleTypes, ALL_SUB_RULE_TYPES);
      case RULE_EVALUATION_POINT_PLATFORM:
        return filterSubRuleTypes(subRuleTypes, ALL_SUB_RULE_TYPES);
      default:
        log.error("Unsupported rule evaluation point : {}", ruleEvaluationPoint);
        throw new IllegalArgumentException(
            "Unsupported rule evaluation point: " + ruleEvaluationPoint);
    }
  }

  private List<AnomalySubRuleType> filterSubRuleTypes(
      List<AnomalySubRuleType> subRuleTypes, List<AnomalySubRuleType> allowedTypes) {
    log.debug(
        "filterSubRuleTypes called with subRuleTypes={}, allowedTypes={}",
        subRuleTypes,
        allowedTypes);
    return subRuleTypes.isEmpty()
        ? allowedTypes
        : subRuleTypes.stream().filter(allowedTypes::contains).collect(Collectors.toList());
  }

  private Optional<WebAppEvaluationConfig> getWebAppEvaluationConfig(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      ModsecRuleVersion modsecRuleVersion,
      RuleEvaluationPoint ruleEvaluationPoint,
      ScopeContext scopeContext,
      RuleVersion ruleVersion) {
    log.debug(
        "getWebAppEvaluationConfig called with modsecRuleVersion={}, ruleEvaluationPoint={}, ruleVersion={}",
        modsecRuleVersion,
        ruleEvaluationPoint,
        ruleVersion);
    Set<String> disabledRuleIds =
        getDisabledModsecRuleIds(
            scopedAnomalyDetectionConfig, modsecRuleVersion, ruleEvaluationPoint, ruleVersion);
    log.debug("Retrieved {} disabled rule IDs", disabledRuleIds.size());
    if (disabledRuleIds.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        WebAppEvaluationConfig.newBuilder()
            .setScopeContext(scopeContext)
            .addAllDisabledSecRuleIds(disabledRuleIds)
            .build());
  }

  private Set<String> getDisabledModsecRuleIds(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      ModsecRuleVersion modsecRuleVersion,
      RuleEvaluationPoint ruleEvaluationPoint,
      RuleVersion ruleVersion) {
    Map<String, AnomalyDetectionConfig> anomalyRuleConfigMap =
        getModsecAnomalyRuleConfigMap(scopedAnomalyDetectionConfig);
    Map<String, AnomalyRuleInfo> ruleInfoMap;
    if (ruleVersion != RuleVersion.getDefaultInstance()) {
      ruleInfoMap =
          webAppRuleInfoProvider.getWebAppRuleInfo(ruleVersion).stream()
              .collect(
                  Collectors.toUnmodifiableMap(AnomalyRuleInfo::getRuleId, Function.identity()));
    } else {
      ruleInfoMap = modsecRulesRegistry.getModsecRuleInfos(modsecRuleVersion, false);
    }
    // The disabled modsec rule ids should be ordered to ensure that
    // the blob doesn't keep changing on repeated calls
    Set<String> disabledModsecRuleIds = new TreeSet<>();

    for (Map.Entry<String, AnomalyRuleInfo> entry : ruleInfoMap.entrySet()) {
      String ruleId = entry.getKey();
      AnomalyRuleInfo anomalyRuleInfo = entry.getValue();
      AnomalyDetectionConfig detectionConfig =
          anomalyRuleConfigMap.getOrDefault(ruleId, AnomalyDetectionConfig.getDefaultInstance());

      Map<String, AnomalySubRuleConfig> subRuleConfigMap = getSubRuleConfigMap(detectionConfig);
      for (AnomalySubRuleInfo subRuleInfo : anomalyRuleInfo.getSubRuleInfosList()) {
        String subRuleId = subRuleInfo.getRuleId();

        AnomalySubRuleConfig subRuleConfig =
            subRuleConfigMap.getOrDefault(subRuleId, AnomalySubRuleConfig.getDefaultInstance());

        boolean isDisabled = isDisabled(detectionConfig, subRuleConfig, ruleEvaluationPoint);
        if (isDisabled) {
          disabledModsecRuleIds.add(removeCrsPrefixIfPresent(subRuleId));
        }
      }
    }
    return disabledModsecRuleIds;
  }

  private boolean isDisabled(
      AnomalyDetectionConfig detectionConfig,
      AnomalySubRuleConfig subRuleConfig,
      RuleEvaluationPoint ruleEvaluationPoint) {
    switch (ruleEvaluationPoint) {
      case RULE_EVALUATION_POINT_EDGE:
        return detectionConfig.getConfigStatus().getDisabled()
            || !subRuleConfig
                .getAnomalyRuleAction()
                .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK);
      case RULE_EVALUATION_POINT_PLATFORM:
        return detectionConfig.getConfigStatus().getDisabled()
            || subRuleConfig
                .getAnomalyRuleAction()
                .equals(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE);
      default:
        log.error("Unsupported rule evaluation point: {}", ruleEvaluationPoint);
        throw new IllegalArgumentException(
            "Unsupported rule evaluation point: " + ruleEvaluationPoint);
    }
  }

  private SecRuleProcessorConfig getSecRuleProcessorConfig(
      ScopedAnomalyConfigStatus configStatus,
      ModsecRuleVersion modsecRuleVersion,
      ScopeContext scopeContext) {
    GlobalModsecConfig globalModsecConfig = configStatus.getGlobalModsecConfig();
    CorazaEngineVersion corazaEngineVersion =
        getCorazaEngineVersion(
            globalModsecConfig.getModsecEvaluationEngineConfig().getCorazaEngineVersion());

    return SecRuleProcessorConfig.newBuilder()
        .setProcessorDetails(getSecRuleProcessorDetails(modsecRuleVersion, corazaEngineVersion))
        .setScopeContext(scopeContext)
        .build();
  }

  private SecRuleProcessorDetails getSecRuleProcessorDetails(
      ModsecRuleVersion ruleVersion, CorazaEngineVersion corazaEngineVersion) {
    switch (ruleVersion) {
      case MODSEC_RULE_VERSION_V3:
      case MODSEC_RULE_VERSION_TEST_V3:
      case MODSEC_RULE_VERSION_SENSITIVE_AGENT_V3:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType.MODSEC_JNI_RULE_DIRECTIVES_TYPE_BASIC))
            .build();
      case MODSEC_RULE_VERSION_V3_SECARG_LIMITS:
      case MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType.MODSEC_JNI_RULE_DIRECTIVES_TYPE_SECARGLIMITS))
            .build();
      case MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
      case MODSEC_RULE_VERSION_TEST_V3_SECARG_LIMITS_DETECTION_ONLY_MODE:
        return SecRuleProcessorDetails.newBuilder()
            .setModsecRuleProcessor(
                ModsecJniRuleProcessor.newBuilder()
                    .setDirectivesType(
                        ModsecJniRuleDirectivesType
                            .MODSEC_JNI_RULE_DIRECTIVES_TYPE_SECARGLIMITS_DETECTION_ONLY))
            .build();
      case MODSEC_RULE_VERSION_CORAZA_V3:
      case MODSEC_RULE_VERSION_TEST_CORAZA_V3:
      case MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3:
        return SecRuleProcessorDetails.newBuilder()
            .setCorazaRuleProcessor(
                CorazaRuleProcessor.newBuilder()
                    .setCorazaEngineVersion(corazaEngineVersion)
                    .setDirectivesType(CorazaRuleDirectivesType.CORAZA_RULE_DIRECTIVES_TYPE_BASIC))
            .build();
      case MODSEC_RULE_VERSION_CORAZA_V3_DETECTION_ONLY_MODE:
      case MODSEC_RULE_VERSION_TEST_CORAZA_V3_DETECTION_ONLY_MODE:
        return SecRuleProcessorDetails.newBuilder()
            .setCorazaRuleProcessor(
                CorazaRuleProcessor.newBuilder()
                    .setCorazaEngineVersion(corazaEngineVersion)
                    .setDirectivesType(
                        CorazaRuleDirectivesType.CORAZA_RULE_DIRECTIVES_TYPE_DETECTION_ONLY))
            .build();
      default:
        log.error("Unsupported ModsecRuleVersion: {}", ruleVersion.name());
        throw new IllegalArgumentException("Unsupported ModsecRuleVersion: " + ruleVersion.name());
    }
  }

  private CorazaEngineVersion getCorazaEngineVersion(
      ai.traceable.anomaly.config.service.v1.global.CorazaEngineVersion corazaEngineVersion) {
    switch (corazaEngineVersion) {
      case CORAZA_ENGINE_VERSION_LATEST_TEST:
        return CorazaEngineVersion.CORAZA_ENGINE_VERSION_LATEST_TEST;
      case CORAZA_ENGINE_VERSION_LATEST_STABLE:
      default:
        return CorazaEngineVersion.CORAZA_ENGINE_VERSION_LATEST_STABLE;
    }
  }

  private ModsecRuleVersion getModsecRuleVersion(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    Optional<ModsecRuleVersion> modsecRuleVersion =
        scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getModsecurityAnomalyDetectionConfig)
            .filter(ModsecurityAnomalyDetectionConfig::hasModsecAllDetection)
            .map(modsecConfig -> modsecConfig.getModsecAllDetection().getModsecRuleVersion())
            .findFirst();
    if (modsecRuleVersion.isPresent()
        && modsecRuleVersion.get() != ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED) {
      return modsecRuleVersion.get();
    }
    return defaultModsecRuleVersion;
  }

  private Map<AnomalyConfigScope, ScopedAnomalyDetectionConfig> getScopedAnomalyDetectionConfigMap(
      RequestContext requestContext) {
    return anomalyDetectionConfigManager
        .getAllGlobalResolvedScopedAnomalyDetectionConfigs(
            requestContext, ANOMALY_DETECTION_CONFIGS_FILTER)
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                ScopedAnomalyDetectionConfig::getConfigScope, Function.identity()));
  }

  private Map<AnomalyConfigScope, ScopedAnomalyConfigStatus> getScopedAnomalyConfigStatusMap(
      RequestContext requestContext) {
    return globalAnomalyConfigStatusManager
        .getAllScopedAnomalyConfigStatusConfigs(requestContext, Collections.emptyList())
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                ScopedAnomalyConfigStatus::getConfigScope, Function.identity()));
  }

  private RuleVersion getRuleVersion(
      RuleVersionData ruleVersionData, RequestContext requestContext) {
    if (featureCachingClient.isWAAPVersioningEnabledForTenant(requestContext)) {
      return ruleVersionData.getCurrentVersion();
    }
    return RuleVersion.getDefaultInstance();
  }

  private String removeCrsPrefixIfPresent(String ruleId) {
    if (ruleId.startsWith(ModsecRuleUtils.MODSEC_RULE_PREFIX)) {
      return ruleId.substring(4);
    }
    return ruleId;
  }

  private LoadingCache<ContextualKey<RequestData>, WebAppEvaluationConfigContext> buildCache(
      ModsecConfigServiceConfig modsecConfigServiceConfig) {
    return CacheBuilder.newBuilder()
        .maximumSize(modsecConfigServiceConfig.getWebAppConfigContextCacheMaxSize())
        .refreshAfterWrite(
            modsecConfigServiceConfig.getWebAppConfigContextCacheRefreshAfterWriteDuration())
        .recordStats()
        .build(
            CacheLoader.asyncReloading(
                createCacheLoader(),
                Executors.newFixedThreadPool(
                    modsecConfigServiceConfig.getWebAppConfigContextCacheThreadPoolSize(),
                    this.buildThreadFactory())));
  }

  private ThreadFactory buildThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("web-app-config-ctxt-cache-%d")
        .build();
  }

  private CacheLoader<ContextualKey<RequestData>, WebAppEvaluationConfigContext>
      createCacheLoader() {
    return new CacheLoader<>() {

      @Override
      public WebAppEvaluationConfigContext load(ContextualKey<RequestData> key) {
        return computeWebAppEvaluationConfigContext(key.getContext(), key.getData().getRequest());
      }

      @Override
      public ListenableFuture<WebAppEvaluationConfigContext> reload(
          ContextualKey<RequestData> cacheKey, WebAppEvaluationConfigContext existingValue) {
        try {
          return Futures.immediateFuture(
              computeWebAppEvaluationConfigContext(
                  cacheKey.getContext(), cacheKey.getData().getRequest()));
        } catch (Exception e) {
          log.error(
              "Failed to reload cache entry for tenant: {}",
              cacheKey.getContext().getTenantId(),
              e);
        }
        return Futures.immediateFuture(existingValue);
      }
    };
  }

  private void registerCacheMetrics(ModsecConfigServiceConfig modsecConfigServiceConfig) {
    PlatformMetricsRegistry.registerCacheTrackingOccupancy(
        CACHE_NAME,
        this.webAppConfigContextCache,
        Collections.emptyMap(),
        modsecConfigServiceConfig.getWebAppConfigContextCacheMaxSize());
  }

  /**
   * Invalidates the cache for a specific tenant. This should be called when anomaly detection
   * configs or global status configs are updated for a tenant.
   *
   * @param tenantId the tenant ID whose cache entries should be invalidated
   */
  public void invalidateCacheForTenant(String tenantId) {
    log.info("Invalidating cache for tenant: {}", tenantId);
    webAppConfigContextCache
        .asMap()
        .keySet()
        .removeIf(
            key ->
                key.getContext().getTenantId().isPresent()
                    && key.getContext().getTenantId().get().equals(tenantId));
  }

  /**
   * Invalidates all cache entries. Use with caution - typically only needed for testing or
   * emergency situations.
   */
  public void invalidateAllCache() {
    log.warn("Invalidating all cache entries");
    webAppConfigContextCache.invalidateAll();
  }

  /**
   * Updates cache based on Kafka config change events. This method is registered as a callback with
   * KafkaLiveEventListener to automatically invalidate cache when relevant configs change.
   *
   * @param key the config change event key containing config type and tenant info
   * @param value the config change event value containing the event type
   */
  private void updateCacheBasedOnEvent(ConfigChangeEventKey key, ConfigChangeEventValue value) {
    if (!key.getConfigType().equals(ScopedAnomalyDetectionConfig.class.getName())
        && !key.getConfigType().equals(ScopedAnomalyConfigStatus.class.getName())) {
      return;
    }

    log.debug(
        "Received config change event for configType={}, tenantId={}, eventType={}",
        key.getConfigType(),
        key.getTenantId(),
        value.getEventCase());

    switch (value.getEventCase()) {
      case CREATE_EVENT:
      case UPDATE_EVENT:
      case DELETE_EVENT:
        invalidateCacheForTenant(key.getTenantId());
        break;
      default:
        log.debug("Ignoring event type: {}", value.getEventCase());
    }
  }

  /** Request data to be used for building cache key. */
  @Value
  @EqualsAndHashCode
  static class RequestData {
    RuleEvaluationPoint ruleEvaluationPoint;
    List<AnomalySubRuleType> subRuleTypes;

    static RequestData from(GetWebAppEvaluationConfigContextRequest request) {
      // Sort sub-rule types for consistent cache keys
      List<AnomalySubRuleType> sortedSubRuleTypes =
          request.getSubRuleTypesList().stream()
              .sorted(Comparator.comparing(Enum::ordinal))
              .collect(Collectors.toList());

      return new RequestData(request.getRuleEvaluationPoint(), sortedSubRuleTypes);
    }

    GetWebAppEvaluationConfigContextRequest getRequest() {
      return GetWebAppEvaluationConfigContextRequest.newBuilder()
          .setRuleEvaluationPoint(ruleEvaluationPoint)
          .addAllSubRuleTypes(subRuleTypes)
          .build();
    }
  }

  /**
   * Helper class to uniquely identify the combination of args/variables that affect the contents of
   * crs rules blob for any scope.
   */
  @Value
  static class RulesContextArgs {
    ModsecRuleVersion modsecRuleVersion;
    List<AnomalySubRuleType> anomalySubRuleTypes;
    RuleVersion ruleVersion;
  }
}
