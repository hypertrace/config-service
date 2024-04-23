package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigConstants.ANOMALY_DETECTION_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigConstants.ANOMALY_DETECTION_CONFIG_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.detector.DetectorConfigServiceConfig;
import ai.traceable.anomaly.config.service.detector.anomalydetection.handler.AnomalyDetectionConfigHandler;
import ai.traceable.anomaly.config.service.global.status.GlobalAnomalyConfigStatusManager;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  private final AnomalyDetectionConfigHandler anomalyDetectionConfigHandler;
  private final AnomalyConfigScopeUtils anomalyConfigScopeUtils;
  private final GlobalAnomalyConfigStatusManager globalAnomalyConfigStatusManager;
  private final List<AnomalyDetectionConfig> defaultModsecConfigs;
  private final List<AnomalyDetectionConfig> defaultApiDefinitionDetectionConfigs;
  private final List<AnomalyDetectionConfig> defaultSessionDefinitionDetectionConfigs;
  private final List<AnomalyDetectionConfig> defaultCustomRulesDetectionConfigs;
  private final List<AnomalyDetectionConfig> defaultVolumetricDetectionConfigs;
  private final List<AnomalyDetectionConfig> defaultCredentialStuffingDetectionConfigs;

  @Inject
  public AnomalyDetectionConfigManagerImpl(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      AnomalyDetectionConfigHandler anomalyDetectionConfigHandler,
      AnomalyConfigScopeUtils anomalyConfigScopeUtils,
      DetectorConfigServiceConfig config,
      ConfigChangeEventGenerator configChangeEventGenerator,
      GlobalAnomalyConfigStatusManager anomalyConfigStatusManager) {
    super(
        configServiceBlockingStub,
        ANOMALY_DETECTION_CONFIG_NAMESPACE,
        ANOMALY_DETECTION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.anomalyDetectionConfigHandler = anomalyDetectionConfigHandler;
    this.anomalyConfigScopeUtils = anomalyConfigScopeUtils;
    this.defaultModsecConfigs = config.getDefaultModsecDetectionConfigs();
    this.defaultApiDefinitionDetectionConfigs = config.getDefaultApiDefinitionDetectionConfigs();
    this.defaultSessionDefinitionDetectionConfigs =
        config.getDefaultSessionDefinitionDetectionConfigs();
    this.defaultCustomRulesDetectionConfigs = config.getDefaultCustomRulesDetectionConfigs();
    this.defaultVolumetricDetectionConfigs = config.getDefaultVolumetricDetectionConfigs();
    this.defaultCredentialStuffingDetectionConfigs =
        config.getDefaultCredentialStuffingDetectionConfigs();
    this.globalAnomalyConfigStatusManager = anomalyConfigStatusManager;
  }

  @Override
  public ScopedAnomalyDetectionConfig getScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetAnomalyDetectionConfigsFilter filter) {
    Map<String, ScopedAnomalyDetectionConfig> configMap = fetchConfigMap(requestContext);
    return getResolvedConfig(
        configMap,
        configScope,
        anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
            getTenantId(requestContext), configScope),
        filter);
  }

  @Override
  public ScopedAnomalyDetectionConfig getGlobalResolvedScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetAnomalyDetectionConfigsFilter filter) {
    Map<String, ScopedAnomalyDetectionConfig> configMap = fetchConfigMap(requestContext);
    ScopedAnomalyDetectionConfig resolvedConfig =
        getResolvedConfig(
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
        globalAnomalyConfigStatusManager.getAllScopedAnomalyConfigStatusConfigs(requestContext);

    Map<AnomalyConfigScope, ScopedAnomalyConfigStatus> globalConfigStatusMap =
        globalConfigStatuses.stream()
            .collect(
                Collectors.toMap(ScopedAnomalyConfigStatus::getConfigScope, Function.identity()));

    return getResolvedConfigs(fetchConfigMap(requestContext), getTenantId(requestContext), filter)
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
        fetchConfigMap(requestContext);

    return getResolvedConfigs(
        anomalyDetectionConfigMap, requestContext.getTenantId().orElseThrow(), filter);
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

    return anomalyDetectionConfigHandler.merge(
        scopedAnomalyDetectionConfig, ScopedAnomalyDetectionConfig.getDefaultInstance(), filter);
  }

  @Override
  public List<ScopedAnomalyDetectionConfig> getAllUnresolvedScopedAnomalyDetectionConfigs(
      RequestContext requestContext, GetAnomalyDetectionConfigsFilter filter) {

    String tenantId = requestContext.getTenantId().orElseThrow();

    Map<String, ScopedAnomalyDetectionConfig> anomalyDetectionConfigMap =
        fetchConfigMap(requestContext);

    List<ScopedAnomalyDetectionConfig> scopedAnomalyDetectionConfigs =
        new ArrayList<>(anomalyDetectionConfigMap.values());
    if (!anomalyDetectionConfigMap.containsKey(tenantId)) {
      scopedAnomalyDetectionConfigs.add(
          ScopedAnomalyDetectionConfig.newBuilder()
              .setConfigScope(anomalyConfigScopeUtils.getDefaultCustomerConfigScope())
              .build());
    }

    scopedAnomalyDetectionConfigs.add(
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(AnomalyConfigScope.getDefaultInstance())
            .addAllAnomalyDetectionConfigs(defaultModsecConfigs)
            .addAllAnomalyDetectionConfigs(defaultApiDefinitionDetectionConfigs)
            .addAllAnomalyDetectionConfigs(defaultSessionDefinitionDetectionConfigs)
            .addAllAnomalyDetectionConfigs(defaultCustomRulesDetectionConfigs)
            .addAllAnomalyDetectionConfigs(defaultVolumetricDetectionConfigs)
            .addAllAnomalyDetectionConfigs(defaultCredentialStuffingDetectionConfigs)
            .build());

    return scopedAnomalyDetectionConfigs.stream()
        .map(
            scopedAnomalyDetectionConfig ->
                anomalyDetectionConfigHandler.merge(
                    scopedAnomalyDetectionConfig,
                    ScopedAnomalyDetectionConfig.getDefaultInstance(),
                    filter))
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

  private Map<String, ScopedAnomalyDetectionConfig> fetchConfigMap(RequestContext requestContext) {
    return getAllObjects(requestContext).stream()
        .collect(Collectors.toMap(ContextualConfigObject::getContext, ConfigObject::getData));
  }

  /**
   * @param configMap
   * @param tenantId
   * @return List of resolved scopedAnomalyDetectionConfigs for all the anomalyConfigScopes of the
   *     given tenant
   */
  private List<ScopedAnomalyDetectionConfig> getResolvedConfigs(
      Map<String, ScopedAnomalyDetectionConfig> configMap,
      String tenantId,
      GetAnomalyDetectionConfigsFilter filter) {

    List<ScopedAnomalyDetectionConfig> resolvedConfigs = new ArrayList<>();
    for (Map.Entry<String, ScopedAnomalyDetectionConfig> entry : configMap.entrySet()) {
      AnomalyConfigScope anomalyConfigScope = entry.getValue().getConfigScope();
      resolvedConfigs.add(
          getResolvedConfig(
              configMap,
              anomalyConfigScope,
              anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
                  tenantId, anomalyConfigScope),
              filter));
    }

    if (!configMap.containsKey(tenantId)) {
      resolvedConfigs.add(
          getResolvedConfig(
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
   * @return ScopedAnomalyDetectionConfig, resolved using the provided context priority.
   */
  private ScopedAnomalyDetectionConfig getResolvedConfig(
      Map<String, ScopedAnomalyDetectionConfig> configMap,
      AnomalyConfigScope configScope,
      List<String> contextsWithIncreasingPriority,
      GetAnomalyDetectionConfigsFilter filter) {
    ScopedAnomalyDetectionConfig anomalyDetectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(configScope)
            .addAllAnomalyDetectionConfigs(defaultModsecConfigs)
            .addAllAnomalyDetectionConfigs(defaultApiDefinitionDetectionConfigs)
            .addAllAnomalyDetectionConfigs(defaultSessionDefinitionDetectionConfigs)
            .addAllAnomalyDetectionConfigs(defaultCustomRulesDetectionConfigs)
            .addAllAnomalyDetectionConfigs(defaultVolumetricDetectionConfigs)
            .addAllAnomalyDetectionConfigs(defaultCredentialStuffingDetectionConfigs)
            .build();
    for (String context : contextsWithIncreasingPriority) {
      anomalyDetectionConfig =
          configMap.containsKey(context)
              ? anomalyDetectionConfigHandler.merge(
                  configMap.get(context), anomalyDetectionConfig, filter)
              : anomalyDetectionConfig;
    }
    return anomalyDetectionConfig;
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
    if (globalConfigStatus
        .flatMap(status -> Optional.of(status.getConfigStatus().getDisabled()))
        .orElse(false)) {
      List<AnomalyDetectionConfig> resolvedAnomalyDetectionConfigs =
          disableAnomalyDetectionConfigs(resolvedConfig.getAnomalyDetectionConfigsList());

      return resolvedConfig.toBuilder()
          .clearAnomalyDetectionConfigs()
          .addAllAnomalyDetectionConfigs(resolvedAnomalyDetectionConfigs)
          .build();
    }

    return resolvedConfig;
  }

  private List<AnomalyDetectionConfig> disableAnomalyDetectionConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .map(
            config -> {
              if (!config.getConfigStatus().getDisabled()) {
                AnomalyConfigStatusChange disableConfigStatus =
                    config.getConfigStatus().toBuilder().setDisabled(true).build();
                return config.toBuilder().setConfigStatus(disableConfigStatus).build();
              }
              return config;
            })
        .collect(Collectors.toList());
  }
}
