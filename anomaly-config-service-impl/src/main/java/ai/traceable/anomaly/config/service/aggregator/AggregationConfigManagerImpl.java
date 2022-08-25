package ai.traceable.anomaly.config.service.aggregator;

import static ai.traceable.anomaly.config.service.aggregator.config.AnomalyAggregationConfigConstants.ANOMALY_AGGREGATION_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.aggregator.config.AnomalyAggregationConfigConstants.ANOMALY_AGGREGATION_CONFIG_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.aggregator.config.AggregationConfigServiceConfig;
import ai.traceable.anomaly.config.service.aggregator.handler.AnomalyAggregationConfigHandler;
import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.aggregator.AggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationFamilyConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationGlobalConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.ScopedAnomalyEventAggregationConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
public class AggregationConfigManagerImpl
    extends IdentifiedObjectStore<ScopedAnomalyEventAggregationConfig>
    implements AggregationConfigManager {

  private final AnomalyAggregationConfigHandler anomalyAggregationConfigHandler;
  private final AnomalyConfigScopeUtils anomalyConfigScopeUtils;
  private final Optional<EventAggregationFamilyConfig> defaultModsecConfig;
  private final Optional<EventAggregationFamilyConfig> defaultApiDefinitionAggregationConfig;
  private final Optional<EventAggregationFamilyConfig> defaultSessionAggregationConfig;
  private final Optional<EventAggregationGlobalConfig> defaultGlobalAggregationConfig;

  @Inject
  public AggregationConfigManagerImpl(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      AnomalyAggregationConfigHandler anomalyAggregationConfigHandler,
      AnomalyConfigScopeUtils anomalyConfigScopeUtils,
      AggregationConfigServiceConfig config,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        ANOMALY_AGGREGATION_CONFIG_NAMESPACE,
        ANOMALY_AGGREGATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.anomalyAggregationConfigHandler = anomalyAggregationConfigHandler;
    this.anomalyConfigScopeUtils = anomalyConfigScopeUtils;
    this.defaultModsecConfig =
        config
            .getDefaultModsecAggregationConfig()
            .flatMap(
                aggregationConfig ->
                    getDefaultConfig(
                        aggregationConfig, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC));
    this.defaultApiDefinitionAggregationConfig =
        config
            .getDefaultApiDefinitionAggregationConfig()
            .flatMap(
                aggregationConfig ->
                    getDefaultConfig(
                        aggregationConfig, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF));
    this.defaultSessionAggregationConfig =
        config
            .getDefaultSessionAggregationConfig()
            .flatMap(
                aggregationConfig ->
                    getDefaultConfig(
                        aggregationConfig, AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION));
    this.defaultGlobalAggregationConfig = config.getDefaultGlobalAggregationConfig();
  }

  @Override
  public ScopedAnomalyEventAggregationConfig updateScopedAnomalyEventAggregationConfig(
      RequestContext requestContext,
      ScopedAnomalyEventAggregationConfig scopedAnomalyEventAggregationConfig) {
    Optional<ScopedAnomalyEventAggregationConfig> currentConfig =
        getData(requestContext, getContextFromData(scopedAnomalyEventAggregationConfig));
    ScopedAnomalyEventAggregationConfig updatedScopedAnomalyAggregationConfig =
        anomalyAggregationConfigHandler.merge(
            scopedAnomalyEventAggregationConfig,
            currentConfig.orElse(ScopedAnomalyEventAggregationConfig.getDefaultInstance()));
    return upsertObject(requestContext, updatedScopedAnomalyAggregationConfig).getData();
  }

  @Override
  public ScopedAnomalyEventAggregationConfig getScopedAnomalyAggregationConfig(
      RequestContext requestContext, AnomalyConfigScope configScope) {
    List<String> contextsWithIncreasingPriority =
        anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
            requestContext.getTenantId().get(), configScope);
    Map<String, ScopedAnomalyEventAggregationConfig> configMap = fetchConfigMap(requestContext);
    return getResolvedConfig(configMap, configScope, contextsWithIncreasingPriority);
  }

  @Override
  public List<ScopedAnomalyEventAggregationConfig> getAllScopedAnomalyEventAggregationConfigs(
      RequestContext requestContext) {
    Map<String, ScopedAnomalyEventAggregationConfig> anomalyAggregationConfigMap =
        fetchConfigMap(requestContext);
    return getResolvedConfigs(
        anomalyAggregationConfigMap, requestContext.getTenantId().orElseThrow());
  }

  @Override
  public ScopedAnomalyEventAggregationConfig getUnresolvedScopedAnomalyEventAggregationConfig(
      RequestContext requestContext, AnomalyConfigScope configScope) {
    return getData(
            requestContext,
            anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(
                requestContext.getTenantId().get(), configScope))
        .orElse(
            ScopedAnomalyEventAggregationConfig.newBuilder().setConfigScope(configScope).build());
  }

  @Override
  public List<ScopedAnomalyEventAggregationConfig>
      getAllUnresolvedScopedAnomalyEventAggregationConfigs(RequestContext requestContext) {
    Map<String, ScopedAnomalyEventAggregationConfig> anomalyAggregationConfigMap =
        fetchConfigMap(requestContext);

    if (anomalyAggregationConfigMap.isEmpty()) {
      return Collections.emptyList();
    }
    return new ArrayList<>(anomalyAggregationConfigMap.values());
  }

  @Override
  public void deleteScopedAnomalyEventAggregationConfig(
      RequestContext requestContext, AnomalyConfigScope anomalyConfigScope) {
    deleteObject(
        requestContext,
        anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(anomalyConfigScope));
  }

  private Map<String, ScopedAnomalyEventAggregationConfig> fetchConfigMap(
      RequestContext requestContext) {
    return getAllObjects(requestContext).stream()
        .collect(Collectors.toMap(ContextualConfigObject::getContext, ConfigObject::getData));
  }

  @Override
  protected Optional<ScopedAnomalyEventAggregationConfig> buildDataFromValue(Value value) {
    try {
      return Optional.of(anomalyAggregationConfigHandler.convert(value));
    } catch (InvalidProtocolBufferException e) {
      log.error(
          "Unable to convert config to ScopedAnomalyEventAggregationConfig for value: {}", value);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ScopedAnomalyEventAggregationConfig data) {
    return anomalyAggregationConfigHandler.convert(data);
  }

  /**
   * @param configMap
   * @param tenantId
   * @return List of resolved scopedAnomalyAggregationConfigs for all the anomalyConfigScopes of the
   *     given tenant
   */
  private List<ScopedAnomalyEventAggregationConfig> getResolvedConfigs(
      Map<String, ScopedAnomalyEventAggregationConfig> configMap, String tenantId) {

    List<ScopedAnomalyEventAggregationConfig> resolvedConfigs = new ArrayList<>();
    for (Map.Entry<String, ScopedAnomalyEventAggregationConfig> entry : configMap.entrySet()) {
      AnomalyConfigScope anomalyConfigScope = entry.getValue().getConfigScope();
      resolvedConfigs.add(
          getResolvedConfig(
              configMap,
              anomalyConfigScope,
              anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
                  tenantId, anomalyConfigScope)));
    }
    return resolvedConfigs;
  }

  @Override
  protected String getContextFromData(ScopedAnomalyEventAggregationConfig data) {
    return anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(data.getConfigScope());
  }

  private Optional<EventAggregationFamilyConfig> getDefaultConfig(
      AggregationConfig defaultAggregationConfig, AnomalyEventFamily anomalyEventFamily) {
    return Optional.of(
        EventAggregationFamilyConfig.newBuilder()
            .setAnomalyEventFamily(anomalyEventFamily)
            .setAggregationConfig(defaultAggregationConfig)
            .build());
  }

  /**
   * @param configMap
   * @param configScope
   * @param contextsWithIncreasingPriority
   * @return ScopedAnomalyEventAggregationConfig, resolved using the provided context priority.
   */
  private ScopedAnomalyEventAggregationConfig getResolvedConfig(
      Map<String, ScopedAnomalyEventAggregationConfig> configMap,
      AnomalyConfigScope configScope,
      List<String> contextsWithIncreasingPriority) {
    ScopedAnomalyEventAggregationConfig anomalyAggregationConfig =
        getDefaultEventAggregationConfig(configScope);
    for (String context : contextsWithIncreasingPriority) {
      anomalyAggregationConfig =
          configMap.containsKey(context)
              ? anomalyAggregationConfigHandler.merge(
                  configMap.get(context), anomalyAggregationConfig)
              : anomalyAggregationConfig;
    }
    return anomalyAggregationConfig;
  }

  private ScopedAnomalyEventAggregationConfig getDefaultEventAggregationConfig(
      AnomalyConfigScope configScope) {
    EventAggregationConfig.Builder eventAggregationConfigBuilder =
        EventAggregationConfig.newBuilder();
    defaultGlobalAggregationConfig.ifPresent(eventAggregationConfigBuilder::setGlobalConfig);
    defaultModsecConfig.ifPresent(eventAggregationConfigBuilder::addFamilyConfigs);
    defaultApiDefinitionAggregationConfig.ifPresent(
        eventAggregationConfigBuilder::addFamilyConfigs);
    defaultSessionAggregationConfig.ifPresent(eventAggregationConfigBuilder::addFamilyConfigs);
    return ScopedAnomalyEventAggregationConfig.newBuilder()
        .setConfigScope(configScope)
        .setEventAggregationConfig(eventAggregationConfigBuilder.build())
        .build();
  }
}
