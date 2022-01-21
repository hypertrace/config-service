package ai.traceable.anomaly.config.service.detector.anomalydetection;

import static ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigConstants.ANOMALY_DETECTION_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.detector.anomalydetection.AnomalyDetectionConfigConstants.ANOMALY_DETECTION_CONFIG_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.detector.DetectorConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AnomalyDetectionConfigManagerImpl
    extends IdentifiedObjectStore<ScopedAnomalyDetectionConfig>
    implements AnomalyDetectionConfigManager {

  private final AnomalyDetectionConfigConverter anomalyDetectionConfigConverter;
  private final List<AnomalyDetectionConfig> defaultModsecConfigs;
  private final List<AnomalyDetectionConfig> defaultApiDefinitionDetectionConfigs;

  @Inject
  public AnomalyDetectionConfigManagerImpl(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      AnomalyDetectionConfigConverter anomalyDetectionConfigConverter,
      DetectorConfigServiceConfig config) {
    super(
        configServiceBlockingStub,
        ANOMALY_DETECTION_CONFIG_NAMESPACE,
        ANOMALY_DETECTION_CONFIG_RESOURCE_NAME);
    this.anomalyDetectionConfigConverter = anomalyDetectionConfigConverter;
    this.defaultModsecConfigs = config.getDefaultModsecDetectionConfigs();
    this.defaultApiDefinitionDetectionConfigs = config.getDefaultApiDefinitionDetectionConfigs();
  }

  @Override
  public ScopedAnomalyDetectionConfig getScopedAnomalyDetectionConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetAnomalyDetectionConfigsFilter filter) {
    /*
     * Precedence Order --> apiConfig > serviceConfig > customerConfig > defaultConfig For example, if
     * apiConfig.disabled = true, we use it; if apiConfig.disabled = false, we use
     * serviceConfig.disabled value and so on.. Similarly for all other config values
     */
    List<String> contextsWithIncreasingPriority = new ArrayList<>();
    contextsWithIncreasingPriority.add(
        requestContext
            .getTenantId()
            .orElseThrow(
                () ->
                    new IllegalArgumentException("Unable to get tenant id from request context")));

    switch (configScope.getScopeCase()) {
      case CUSTOMER_SCOPE:
        break;
      case SERVICE_SCOPE:
        contextsWithIncreasingPriority.add(configScope.getServiceScope().getId());
        break;
      case API_SCOPE:
        contextsWithIncreasingPriority.add(configScope.getApiScope().getServiceScope().getId());
        contextsWithIncreasingPriority.add(configScope.getApiScope().getId());
        break;
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", configScope.getScopeCase()));
    }

    Map<String, ScopedAnomalyDetectionConfig> configMap = fetchConfigMap(requestContext);

    ScopedAnomalyDetectionConfig anomalyDetectionConfig =
        getResolvedConfig(configMap, configScope, contextsWithIncreasingPriority);

    Set<AnomalyDetectionConfig.AnomalyDetectionConfigCase> configCases =
        anomalyDetectionConfigConverter.convert(filter);

    return filterConfigs(anomalyDetectionConfig, configCases);
  }

  @Override
  public ScopedAnomalyDetectionConfig updateScopedAnomalyDetectionConfig(
      RequestContext requestContext, ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    Optional<ScopedAnomalyDetectionConfig> currentConfig =
        getData(requestContext, getContextFromData(scopedAnomalyDetectionConfig));
    ScopedAnomalyDetectionConfig updatedScopedAnomalyDetectionConfig =
        anomalyDetectionConfigConverter.merge(
            scopedAnomalyDetectionConfig,
            currentConfig.orElse(ScopedAnomalyDetectionConfig.getDefaultInstance()));

    return upsertObject(requestContext, updatedScopedAnomalyDetectionConfig).getData();
  }

  @Override
  public List<ScopedAnomalyDetectionConfig> getAllScopedAnomalyDetectionConfig(
      RequestContext requestContext, GetAnomalyDetectionConfigsFilter filter) {
    Map<String, ScopedAnomalyDetectionConfig> anomalyDetectionConfigMap =
        fetchConfigMap(requestContext);

    List<ScopedAnomalyDetectionConfig> anomalyDetectionConfigs =
        getResolvedConfigs(anomalyDetectionConfigMap, requestContext.getTenantId().orElseThrow());

    Set<AnomalyDetectionConfig.AnomalyDetectionConfigCase> configCases =
        anomalyDetectionConfigConverter.convert(filter);

    return anomalyDetectionConfigs.stream()
        .map(detectionConfig -> filterConfigs(detectionConfig, configCases))
        .collect(Collectors.toList());
  }

  @Override
  protected Optional<ScopedAnomalyDetectionConfig> buildDataFromValue(Value value) {
    try {
      return Optional.of(anomalyDetectionConfigConverter.convert(value));
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert config to ScopedAnomalyDetectionConfig for value: {}", value);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(ScopedAnomalyDetectionConfig data) {
    return anomalyDetectionConfigConverter.convert(data);
  }

  @Override
  protected String getContextFromData(ScopedAnomalyDetectionConfig data) {
    return getContextFromAnomalyConfigScope(data.getConfigScope());
  }

  private Map<String, ScopedAnomalyDetectionConfig> fetchConfigMap(RequestContext requestContext) {
    return getAllObjects(requestContext).stream()
        .collect(Collectors.toMap(ContextualConfigObject::getContext, ConfigObject::getData));
  }

  private String getContextFromAnomalyConfigScope(AnomalyConfigScope anomalyConfigScope) {
    String context;
    switch (anomalyConfigScope.getScopeCase()) {
      case CUSTOMER_SCOPE:
        context =
            RequestContext.CURRENT
                .get()
                .getTenantId()
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Unable to get tenant id from request context"));
        break;
      case SERVICE_SCOPE:
        context = anomalyConfigScope.getServiceScope().getId();
        break;
      case API_SCOPE:
        context = anomalyConfigScope.getApiScope().getId();
        break;
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", anomalyConfigScope.getScopeCase()));
    }
    return context;
  }

  /**
   * @param configMap
   * @param tenantId
   * @return List of resolved scopedAnomalyDetectionConfigs for all the anomalyConfigScopes of the
   *     given tenant
   */
  private List<ScopedAnomalyDetectionConfig> getResolvedConfigs(
      Map<String, ScopedAnomalyDetectionConfig> configMap, String tenantId) {

    List<ScopedAnomalyDetectionConfig> resolvedConfigs = new ArrayList<>();
    for (Map.Entry<String, ScopedAnomalyDetectionConfig> entry : configMap.entrySet()) {

      AnomalyConfigScope anomalyConfigScope = entry.getValue().getConfigScope();
      List<String> contextsWithIncreasingPriority = new ArrayList<>();
      contextsWithIncreasingPriority.add(tenantId);
      switch (anomalyConfigScope.getScopeCase()) {
        case SERVICE_SCOPE:
          contextsWithIncreasingPriority.add(anomalyConfigScope.getServiceScope().getId());
          break;
        case API_SCOPE:
          contextsWithIncreasingPriority.add(
              anomalyConfigScope.getApiScope().getServiceScope().getId());
          contextsWithIncreasingPriority.add(anomalyConfigScope.getApiScope().getId());
          break;
        default:
          break;
      }

      resolvedConfigs.add(
          getResolvedConfig(configMap, anomalyConfigScope, contextsWithIncreasingPriority));
    }

    if (!configMap.containsKey(tenantId)) {
      resolvedConfigs.add(
          getResolvedConfig(
              Map.of(),
              AnomalyConfigScope.newBuilder()
                  .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                  .build(),
              List.of()));
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
      List<String> contextsWithIncreasingPriority) {
    ScopedAnomalyDetectionConfig anomalyDetectionConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(configScope)
            .addAllAnomalyDetectionConfigs(defaultModsecConfigs)
            .addAllAnomalyDetectionConfigs(defaultApiDefinitionDetectionConfigs)
            .build();
    for (String context : contextsWithIncreasingPriority) {
      anomalyDetectionConfig =
          configMap.containsKey(context)
              ? anomalyDetectionConfigConverter.merge(
                  configMap.get(context), anomalyDetectionConfig)
              : anomalyDetectionConfig;
    }
    return anomalyDetectionConfig;
  }

  private ScopedAnomalyDetectionConfig filterConfigs(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig,
      Set<AnomalyDetectionConfig.AnomalyDetectionConfigCase> configCases) {
    if (configCases.isEmpty()) {
      return scopedAnomalyDetectionConfig;
    }

    ScopedAnomalyDetectionConfig.Builder builder = ScopedAnomalyDetectionConfig.newBuilder();
    builder.setConfigScope(scopedAnomalyDetectionConfig.getConfigScope());
    List<AnomalyDetectionConfig> anomalyDetectionConfigs =
        scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList();
    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    for (AnomalyDetectionConfig.AnomalyDetectionConfigCase configCase : configCases) {
      filteredConfigs.addAll(
          anomalyDetectionConfigs.stream()
              .filter(
                  anomalyDetectionConfig ->
                      anomalyDetectionConfig.getAnomalyDetectionConfigCase() == configCase)
              .collect(Collectors.toList()));
    }

    return builder.addAllAnomalyDetectionConfigs(filteredConfigs).build();
  }
}
