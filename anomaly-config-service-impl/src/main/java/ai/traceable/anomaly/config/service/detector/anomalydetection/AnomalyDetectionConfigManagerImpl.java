package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigConstants.ANOMALY_DETECTION_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigConstants.ANOMALY_DETECTION_CONFIG_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.common.AnomalySubRuleConfigUtils;
import ai.traceable.anomaly.config.service.detector.DetectorConfigServiceConfig;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.AnomalyDetectionConfigHandler;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.GlobalTestingModeResolver;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.ApiDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ModsecDefaultConfigsType;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyDetectionConfigManagerImpl
    extends IdentifiedObjectStore<ScopedAnomalyDetectionConfig>
    implements AnomalyDetectionConfigManager {

  public static final AnomalyConfigScope ANOMALY_CONFIG_CUSTOMER_SCOPE =
      AnomalyConfigScope.newBuilder().setCustomerScope(AnomalyCustomerScope.newBuilder()).build();
  private final AnomalyDetectionConfigHandler anomalyDetectionConfigHandler;
  private final AnomalyConfigScopeUtils anomalyConfigScopeUtils;
  private final GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;
  private final List<AnomalyDetectionConfig> defaultApiProtectionDetectionConfigs;
  private final WafConfigResolver wafConfigResolver;
  private final GlobalTestingModeResolver globalTestingModeResolver;

  @Inject
  public AnomalyDetectionConfigManagerImpl(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      AnomalyDetectionConfigHandler anomalyDetectionConfigHandler,
      AnomalyConfigScopeUtils anomalyConfigScopeUtils,
      DetectorConfigServiceConfig config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      GlobalAnomalyConfigStatusManager anomalyConfigStatusManager,
      WafConfigResolver wafConfigResolver,
      GlobalTestingModeResolver globalTestingModeResolver) {
    super(
        configServiceBlockingStub,
        ANOMALY_DETECTION_CONFIG_NAMESPACE,
        ANOMALY_DETECTION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.anomalyDetectionConfigHandler = anomalyDetectionConfigHandler;
    this.anomalyConfigScopeUtils = anomalyConfigScopeUtils;
    this.defaultApiProtectionDetectionConfigs = config.getDefaultApiProtectionDetectionConfigs();
    this.globalAnomalyConfigStatusManager = anomalyConfigStatusManager;
    this.wafConfigResolver = wafConfigResolver;
    this.globalTestingModeResolver = globalTestingModeResolver;
  }

  @Override
  public ScopedAnomalyDetectionConfig getScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetAnomalyDetectionConfigsFilter filter) {
    Map<String, ScopedAnomalyDetectionConfig> configMap =
        getFilteredConfigMap(requestContext, filter.getApplicableScopesList());
    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig =
        getResolvedConfig(
            requestContext,
            configMap,
            configScope,
            anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
                getTenantId(requestContext), configScope),
            filter);

    Optional<ScopedAnomalyConfigStatus> globalConfigStatus =
        Optional.ofNullable(
            globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
                requestContext, configScope));

    Optional<ScopedAnomalyDetectionConfig>
        resolvedScopedAnomalyDetectionConfigWithGlobalTestingMode =
            globalTestingModeResolver.resolveGlobalTestingMode(
                scopedAnomalyDetectionConfig, globalConfigStatus);
    return resolvedScopedAnomalyDetectionConfigWithGlobalTestingMode.orElse(
        scopedAnomalyDetectionConfig);
  }

  @Override
  public ScopedAnomalyDetectionConfig getGlobalResolvedScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetAnomalyDetectionConfigsFilter filter) {
    Map<String, ScopedAnomalyDetectionConfig> configMap =
        getFilteredConfigMap(requestContext, filter.getApplicableScopesList());
    ScopedAnomalyDetectionConfig resolvedConfig =
        getResolvedConfig(
            requestContext,
            configMap,
            configScope,
            anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
                getTenantId(requestContext), configScope),
            filter);

    Optional<ScopedAnomalyConfigStatus> globalConfigStatus =
        Optional.ofNullable(
            globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
                requestContext, configScope));

    return getResolvedConfig(resolvedConfig, globalConfigStatus);
  }

  @Override
  public List<ScopedAnomalyDetectionConfig> getAllGlobalResolvedScopedAnomalyDetectionConfigs(
      RequestContext requestContext, GetAnomalyDetectionConfigsFilter filter) {
    List<ScopedAnomalyConfigStatus> globalConfigStatuses =
        globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            requestContext, filter.getApplicableScopesList());

    Map<AnomalyConfigScope, ScopedAnomalyConfigStatus> globalConfigStatusMap =
        globalConfigStatuses.stream()
            .collect(
                Collectors.toMap(ScopedAnomalyConfigStatus::getConfigScope, Function.identity()));

    return getResolvedConfigs(
            requestContext,
            getFilteredConfigMap(requestContext, filter.getApplicableScopesList()),
            filter)
        .stream()
        .map(
            resolvedConfig ->
                getResolvedConfig(
                    resolvedConfig,
                    Optional.ofNullable(
                        globalConfigStatusMap.get(resolvedConfig.getConfigScope()))))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public ScopedAnomalyDetectionConfig updateScopedAnomalyDetectionConfig(
      RequestContext requestContext, ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    Optional<ScopedAnomalyDetectionConfig> currentConfig =
        getData(requestContext, getContextFromData(scopedAnomalyDetectionConfig));
    ScopedAnomalyDetectionConfig updatedScopedAnomalyDetectionConfig =
        anomalyDetectionConfigHandler.merge(
            scopedAnomalyDetectionConfig,
            currentConfig.orElse(ScopedAnomalyDetectionConfig.getDefaultInstance()));
    return upsertObject(requestContext, updatedScopedAnomalyDetectionConfig).getData();
  }

  @Override
  public List<ScopedAnomalyDetectionConfig> getAllScopedAnomalyDetectionConfig(
      RequestContext requestContext, GetAnomalyDetectionConfigsFilter filter) {
    Map<String, ScopedAnomalyDetectionConfig> anomalyDetectionConfigMap =
        getFilteredConfigMap(requestContext, filter.getApplicableScopesList());

    List<ScopedAnomalyConfigStatus> globalConfigStatuses =
        globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(
            requestContext, filter.getApplicableScopesList());

    Map<AnomalyConfigScope, ScopedAnomalyConfigStatus> globalConfigStatusMap =
        globalConfigStatuses.stream()
            .collect(
                Collectors.toMap(ScopedAnomalyConfigStatus::getConfigScope, Function.identity()));

    return getResolvedConfigs(requestContext, anomalyDetectionConfigMap, filter).stream()
        .map(
            resolvedConfig ->
                globalTestingModeResolver
                    .resolveGlobalTestingMode(
                        resolvedConfig,
                        Optional.ofNullable(
                            globalConfigStatusMap.get(resolvedConfig.getConfigScope())))
                    .orElse(resolvedConfig))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public ScopedAnomalyDetectionConfig getUnresolvedScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetAnomalyDetectionConfigsFilter filter) {

    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig =
        getData(
                requestContext,
                anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(configScope))
            .orElse(ScopedAnomalyDetectionConfig.newBuilder().setConfigScope(configScope).build());

    scopedAnomalyDetectionConfig =
        anomalyDetectionConfigHandler.merge(
            scopedAnomalyDetectionConfig, ScopedAnomalyDetectionConfig.getDefaultInstance());
    return filterConfigs(scopedAnomalyDetectionConfig, filter);
  }

  @Override
  public List<ScopedAnomalyDetectionConfig> getAllUnresolvedScopedAnomalyDetectionConfigs(
      RequestContext requestContext, GetAnomalyDetectionConfigsFilter filter) {

    String tenantId = requestContext.getTenantId().orElseThrow();

    Map<String, ScopedAnomalyDetectionConfig> anomalyDetectionConfigMap =
        getFilteredConfigMap(requestContext, filter.getApplicableScopesList());

    List<ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigs =
        new ArrayList<>(anomalyDetectionConfigMap.values());
    if (!anomalyDetectionConfigMap.containsKey(tenantId)) {
      scopedAnomalyDetectionConfigs.add(
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(anomalyConfigScopeUtils.getDefaultCustomerConfigScope())
              .build());
    }
    ScopedAnomalyConfigStatus scopedAnomalyConfigStatus =
        globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, ANOMALY_CONFIG_CUSTOMER_SCOPE);
    scopedAnomalyDetectionConfigs.add(
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(AnomalyConfigScope.getDefaultInstance())
            .addAllAnomalyDetectionConfigs(
                wafConfigResolver.resolve(
                    requestContext,
                    scopedAnomalyConfigStatus.getGlobalModsecConfig(),
                    anomalyDetectionConfigMap,
                    Collections.emptyList()))
            .addAllAnomalyDetectionConfigs(
                getDefaultApiProtectionDetectionConfigs(
                    scopedAnomalyConfigStatus.getApiGlobalConfig().getDefaultConfigsType()))
            .build());

    return scopedAnomalyDetectionConfigs.stream()
        .map(
            scopedAnomalyDetectionConfig ->
                anomalyDetectionConfigHandler.merge(
                    scopedAnomalyDetectionConfig,
                    ScopedAnomalyDetectionConfig.getDefaultInstance()))
        .map(detectionConfig -> filterConfigs(detectionConfig, filter))
        .collect(Collectors.toList());
  }

  @Override
  public ScopedAnomalyDetectionConfig deleteScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      ScopedAnomalyDetectionConfig deleteScopedAnomalyDetectionConfig,
      DeleteAnomalyConfigOption deleteAnomalyConfigOption) {

    AnomalyConfigScope configScope = deleteScopedAnomalyDetectionConfig.getConfigScope();
    List<AnomalyDetectionConfig> detectionConfigs =
        deleteScopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList();

    ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig =
        getData(requestContext, getContextFromData(deleteScopedAnomalyDetectionConfig))
            .orElse(ScopedAnomalyDetectionConfig.newBuilder().setConfigScope(configScope).build());

    ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder =
        ScopedAnomalyDetectionConfig.newBuilder();
    deletedConfigBuilder.setConfigScope(configScope);

    if (deleteAnomalyConfigOption.equals(
        DeleteAnomalyConfigOption.DELETE_ANOMALY_CONFIG_OPTION_WHOLE_DETECTION_CONFIG)) {
      upsertObject(
          requestContext,
          anomalyDetectionConfigHandler.deleteWholeAnomalyDetectionConfigs(
              scopedAnomalyDetectionConfig, detectionConfigs, deletedConfigBuilder));
    }

    return deletedConfigBuilder.build();
  }

  @Override
  protected Optional<ScopedAnomalyDetectionConfig> buildDataFromValue(Value value) {
    try {
      return Optional.of(anomalyDetectionConfigHandler.convert(value));
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert config to ScopedAnomalyDetectionConfig for value: {}", value);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(ScopedAnomalyDetectionConfig data) {
    return anomalyDetectionConfigHandler.convert(data);
  }

  @Override
  protected String getContextFromData(ScopedAnomalyDetectionConfig data) {
    return anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(data.getConfigScope());
  }

  private Map<String, ScopedAnomalyDetectionConfig> getFilteredConfigMap(
      RequestContext requestContext, List<AnomalyConfigScope> applicableScopesList) {
    Map<String, ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigMap =
        getAllObjects(requestContext).stream()
            .collect(
                Collectors.toMap(
                    ContextualConfigObject::getContext,
                    ConfigObject::getData,
                    (previous, current) ->
                        previous // sorted by latest in getAllObjects so keep the previous entry
                    ));
    return anomalyConfigScopeUtils.filterConfigMap(
        scopedAnomalyDetectionConfigMap,
        applicableScopesList,
        ScopedAnomalyDetectionConfig::getConfigScope);
  }

  /**
   * @param configMap
   * @param requestContext
   * @param filter
   * @return List of resolved scopedAnomalyDetectionConfigs for all the anomalyConfigScopes of the
   *     given tenant
   */
  private List<ScopedAnomalyDetectionConfig> getResolvedConfigs(
      RequestContext requestContext,
      Map<String, ScopedAnomalyDetectionConfig> configMap,
      GetAnomalyDetectionConfigsFilter filter) {

    List<ScopedAnomalyDetectionConfig> resolvedConfigs = new ArrayList<>();
    String tenantId = requestContext.getTenantId().orElseThrow();
    for (Map.Entry<String, ScopedAnomalyDetectionConfig> entry : configMap.entrySet()) {
      AnomalyConfigScope anomalyConfigScope = entry.getValue().getConfigScope();
      resolvedConfigs.add(
          getResolvedConfig(
              requestContext,
              configMap,
              anomalyConfigScope,
              anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
                  tenantId, anomalyConfigScope),
              filter));
    }

    if (!configMap.containsKey(tenantId)) {
      resolvedConfigs.add(
          getResolvedConfig(
              requestContext,
              Map.of(),
              AnomalyConfigScope.newBuilder()
                  .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                  .build(),
              List.of(),
              filter));
    }
    return resolvedConfigs;
  }

  /**
   * @param configMap
   * @param configScope
   * @param contextsWithIncreasingPriority
   * @param filter
   * @return ScopedAnomalyDetectionConfig, resolved using the provided context priority.
   */
  private ScopedAnomalyDetectionConfig getResolvedConfig(
      RequestContext requestContext,
      Map<String, ScopedAnomalyDetectionConfig> configMap,
      AnomalyConfigScope configScope,
      List<String> contextsWithIncreasingPriority,
      GetAnomalyDetectionConfigsFilter filter) {

    ScopedAnomalyDetectionConfig anomalyDetectionConfig =
        getResolvedScopedAnomalyDetectionConfig(
            requestContext, configMap, configScope, contextsWithIncreasingPriority);
    for (String context : contextsWithIncreasingPriority) {
      anomalyDetectionConfig =
          configMap.containsKey(context)
              ? anomalyDetectionConfigHandler.merge(configMap.get(context), anomalyDetectionConfig)
              : anomalyDetectionConfig;
    }
    return filterConfigs(anomalyDetectionConfig, filter);
  }

  /**
   * for all envs: modsecDefaultConfigType (profile) < config from helm < manual overrides and for
   * specific env, if profile is all-envs, then modsecDefaultConfigType (all-env-profile) < config
   * from helm < manual overrides in all env < manual overrides in specific env if profile is
   * something else, then modsecDefaultConfigType (specific-env-profile) < config from helm < manual
   * overrides in specific env *
   */
  private ScopedAnomalyDetectionConfig getResolvedScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      Map<String, ScopedAnomalyDetectionConfig> configMap,
      AnomalyConfigScope configScope,
      List<String> contextsWithIncreasingPriority) {
    ScopedAnomalyDetectionConfig anomalyDetectionConfig;
    ScopedAnomalyConfigStatus scopedAnomalyConfigStatus =
        globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(requestContext, configScope);
    ScopedAnomalyConfigStatus globalScopedAnomalyConfigStatus =
        globalAnomalyConfigStatusManager.getScopedAnomalyConfigStatus(
            requestContext, ANOMALY_CONFIG_CUSTOMER_SCOPE);
    String tenantId = requestContext.getTenantId().orElseThrow();
    if (configScope.hasEnvironmentScope()
        && !scopedAnomalyConfigStatus
            .getGlobalModsecConfig()
            .getDefaultConfigsType()
            .equals(ModsecDefaultConfigsType.MODSEC_DEFAULT_CONFIGS_TYPE_ALL_ENVIRONMENT)) {
      anomalyDetectionConfig =
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(configScope)
              .addAllAnomalyDetectionConfigs(
                  wafConfigResolver.resolve(
                      requestContext,
                      scopedAnomalyConfigStatus.getGlobalModsecConfig(),
                      configMap,
                      Collections.emptyList()))
              .addAllAnomalyDetectionConfigs(
                  getDefaultApiProtectionDetectionConfigs(
                      globalScopedAnomalyConfigStatus.getApiGlobalConfig().getDefaultConfigsType()))
              .build();
      // tenant resolution not needed as env profile is at higher precedence
      contextsWithIncreasingPriority.remove(tenantId);
    } else {
      anomalyDetectionConfig =
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(configScope)
              .addAllAnomalyDetectionConfigs(
                  wafConfigResolver.resolve(
                      requestContext,
                      globalScopedAnomalyConfigStatus.getGlobalModsecConfig(),
                      configMap,
                      contextsWithIncreasingPriority))
              .addAllAnomalyDetectionConfigs(
                  getDefaultApiProtectionDetectionConfigs(
                      globalScopedAnomalyConfigStatus.getApiGlobalConfig().getDefaultConfigsType()))
              .build();
    }
    return anomalyDetectionConfig;
  }

  private List<AnomalyDetectionConfig> getDefaultApiProtectionDetectionConfigs(
      ApiDefaultConfigsType defaultConfigsType) {
    if (defaultConfigsType.equals(
        ApiDefaultConfigsType.API_DEFAULT_CONFIGS_TYPE_ONLY_API_DEF_ENABLED)) {
      return defaultApiProtectionDetectionConfigs;
    }
    log.error("Invalid api default configs type {}", defaultConfigsType);
    return List.of();
  }

  private final String getTenantId(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get tenant id from request context"));
  }

  /**
   * This method is used to get the global resolved configurations for anomaly detection configs.
   * For more details, refer to the wiki:
   * https://traceableai.atlassian.net/wiki/spaces/Engineering/pages/1812234269/Internal+Excluded+Security+events
   */
  public ScopedAnomalyDetectionConfig getResolvedConfig(
      ScopedAnomalyDetectionConfig resolvedConfig,
      Optional<ScopedAnomalyConfigStatus> globalConfigStatus) {

    // If global config is disabled, then disable all the individual anomaly detection configs.
    boolean globalModsecConfigDisabled =
        globalConfigStatus
            .flatMap(status -> Optional.of(status.getGlobalModsecConfig().getDisabled()))
            .orElse(false);
    boolean apiGlobalConfigDisabled =
        globalConfigStatus
            .flatMap(status -> Optional.of(status.getApiGlobalConfig().getDisabled()))
            .orElse(false);
    if (globalModsecConfigDisabled || apiGlobalConfigDisabled) {
      List<AnomalyDetectionConfig> resolvedAnomalyDetectionConfigs =
          disableAnomalyDetectionConfigs(
              resolvedConfig.getAnomalyDetectionConfigsList(),
              globalModsecConfigDisabled,
              apiGlobalConfigDisabled);

      return resolvedConfig.toBuilder()
          .clearAnomalyDetectionConfigs()
          .addAllAnomalyDetectionConfigs(resolvedAnomalyDetectionConfigs)
          .build();
    }

    Optional<ScopedAnomalyDetectionConfig> resolvedScopedAnomalyDetectionConfig =
        globalTestingModeResolver.resolveGlobalTestingMode(resolvedConfig, globalConfigStatus);
    return resolvedScopedAnomalyDetectionConfig.orElse(resolvedConfig);
  }

  private List<AnomalyDetectionConfig> disableAnomalyDetectionConfigs(
      List<AnomalyDetectionConfig> detectionConfigs,
      boolean modsecGlobalConfigDisabled,
      boolean apiGlobalConfigDisabled) {
    return detectionConfigs.stream()
        .map(
            config -> {
              if (((config.hasModsecurityAnomalyDetectionConfig() && modsecGlobalConfigDisabled)
                      || (!config.hasModsecurityAnomalyDetectionConfig()
                          && apiGlobalConfigDisabled))
                  && !config.getConfigStatus().getDisabled()) {
                AnomalyConfigStatusChange disableConfigStatus =
                    config.getConfigStatus().toBuilder().setDisabled(true).build();
                return config.toBuilder().setConfigStatus(disableConfigStatus).build();
              }
              return config;
            })
        .collect(Collectors.toList());
  }

  private ScopedAnomalyDetectionConfig populateNewFields(
      ScopedAnomalyDetectionConfig scopedConfig) {
    if (scopedConfig == null) {
      return null;
    }

    ScopedAnomalyDetectionConfig.Builder builder =
        scopedConfig.toBuilder().clearAnomalyDetectionConfigs();
    for (AnomalyDetectionConfig config : scopedConfig.getAnomalyDetectionConfigsList()) {
      AnomalyDetectionConfig processedConfig = config;
      if (processedConfig.hasModsecurityAnomalyDetectionConfig()
          && processedConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule()) {

        ModsecurityAnomalyDetectionConfig modsecConfig =
            processedConfig.getModsecurityAnomalyDetectionConfig();
        ModsecurityAnomalyRuleConfig ruleConfig = modsecConfig.getModsecAnomalyRule();
        List<AnomalySubRuleConfig> processedSubRules =
            ruleConfig.getSubRuleConfigsList().stream()
                .map(AnomalySubRuleConfigUtils::populateNewFields)
                .collect(Collectors.toList());
        ModsecurityAnomalyRuleConfig processedRuleConfig =
            ruleConfig.toBuilder()
                .clearSubRuleConfigs()
                .addAllSubRuleConfigs(processedSubRules)
                .build();
        ModsecurityAnomalyDetectionConfig processedModsecConfig =
            modsecConfig.toBuilder().setModsecAnomalyRule(processedRuleConfig).build();

        processedConfig =
            config.toBuilder().setModsecurityAnomalyDetectionConfig(processedModsecConfig).build();
      }

      builder.addAnomalyDetectionConfigs(processedConfig);
    }

    return builder.build();
  }

  private ScopedAnomalyDetectionConfig filterConfigs(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      GetAnomalyDetectionConfigsFilter filter) {
    scopedAnomalyDetectionConfig = populateNewFields(scopedAnomalyDetectionConfig);
    Set<AnomalyDetectionConfig.AnomalyDetectionConfigCase> configCases =
        anomalyDetectionConfigHandler.convert(filter);
    if (configCases.isEmpty()) {
      return scopedAnomalyDetectionConfig;
    }
    ScopedAnomalyDetectionConfig.Builder builder = ScopedAnomalyDetectionConfig.newBuilder();
    builder.setConfigScope(scopedAnomalyDetectionConfig.getConfigScope());
    scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(
            detectionConfig ->
                configCases.contains(detectionConfig.getAnomalyDetectionConfigCase()))
        .forEach(builder::addAnomalyDetectionConfigs);
    return builder.build();
  }
}
