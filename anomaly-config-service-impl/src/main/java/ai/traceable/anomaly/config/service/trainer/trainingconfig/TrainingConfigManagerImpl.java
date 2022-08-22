package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import static ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigConstants.TRAINING_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.trainer.trainingconfig.TrainingConfigConstants.TRAINING_CONFIG_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.trainer.TrainerConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.trainer.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class TrainingConfigManagerImpl extends IdentifiedObjectStore<ScopedTrainingConfig>
    implements TrainingConfigManager {
  private final TrainingConfigHandler configHandler;
  private final List<TrainingConfig> defaultApiNamingTrainingConfigs;
  private final List<TrainingConfig> defaultMetadataTrainingConfigs;

  @Inject
  public TrainingConfigManagerImpl(
      TrainingConfigHandler configHandler,
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      TrainerConfigServiceConfig config,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        TRAINING_CONFIG_NAMESPACE,
        TRAINING_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.configHandler = configHandler;
    this.defaultApiNamingTrainingConfigs = config.getApiNamingTrainingConfigs();
    this.defaultMetadataTrainingConfigs = config.getMetadataTrainingConfigs();
  }

  @Override
  protected Optional<ScopedTrainingConfig> buildDataFromValue(Value value) {
    try {
      return Optional.of(configHandler.convert(value));
    } catch (InvalidProtocolBufferException exception) {
      log.error("Unable to convert config to ScopedTrainingConfig for value: {}", value);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(ScopedTrainingConfig data) {
    return configHandler.convert(data);
  }

  @Override
  protected String getContextFromData(ScopedTrainingConfig data) {
    return getContextFromAnomalyConfigScope(data.getConfigScope());
  }

  @Override
  public ScopedTrainingConfig getScopedTrainingConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetTrainingConfigsFilter filter) {
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

    Map<String, ScopedTrainingConfig> configMap = fetchConfigMap(requestContext);

    ScopedTrainingConfig trainingConfig =
        getResolvedConfig(configMap, configScope, contextsWithIncreasingPriority);

    if (trainingConfig.equals(ScopedTrainingConfig.getDefaultInstance())) {
      trainingConfig = ScopedTrainingConfig.newBuilder().setConfigScope(configScope).build();
    }

    Set<TrainingConfig.TrainingConfigCase> configCases = configHandler.convert(filter);

    return filterConfigs(trainingConfig, configCases);
  }

  @Override
  public List<ScopedTrainingConfig> getAllScopedTrainingConfig(
      RequestContext requestContext, GetTrainingConfigsFilter filter) {
    Map<String, ScopedTrainingConfig> trainingConfigMap = fetchConfigMap(requestContext);

    List<ScopedTrainingConfig> trainingConfigs =
        getResolvedConfigs(trainingConfigMap, requestContext.getTenantId().orElseThrow());

    Set<TrainingConfig.TrainingConfigCase> configCases = configHandler.convert(filter);

    return trainingConfigs.stream()
        .map(trainingConfig -> filterConfigs(trainingConfig, configCases))
        .collect(Collectors.toList());
  }

  @Override
  public ScopedTrainingConfig getUnresolvedTrainingConfig(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      GetTrainingConfigsFilter filter) {
    String context = getContextFromAnomalyConfigScope(configScope);
    ScopedTrainingConfig scopedTrainingConfig =
        getData(requestContext, context)
            .orElse(ScopedTrainingConfig.newBuilder().setConfigScope(configScope).build());
    Set<TrainingConfig.TrainingConfigCase> configCases = configHandler.convert(filter);
    return filterConfigs(scopedTrainingConfig, configCases);
  }

  @Override
  public List<ScopedTrainingConfig> getAllUnresolvedTrainingConfig(
      RequestContext requestContext, GetTrainingConfigsFilter filter) {
    List<ScopedTrainingConfig> scopedTrainingConfigs =
        new ArrayList<>(fetchConfigMap(requestContext).values());
    scopedTrainingConfigs.add(
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(AnomalyConfigScope.getDefaultInstance())
            .addAllTrainingConfigs(getDefaultTrainingConfigs())
            .build());
    Set<TrainingConfig.TrainingConfigCase> configCases = configHandler.convert(filter);
    return scopedTrainingConfigs.stream()
        .map(trainingConfig -> filterConfigs(trainingConfig, configCases))
        .collect(Collectors.toList());
  }

  @Override
  public ScopedTrainingConfig deleteTrainingConfig(
      RequestContext requestContext,
      ScopedTrainingConfig deleteScopedTrainingConfig,
      DeleteAnomalyConfigOption deleteAnomalyConfigOption) {
    AnomalyConfigScope configScope = deleteScopedTrainingConfig.getConfigScope();

    List<TrainingConfig> deleteTrainingConfigFilters =
        deleteScopedTrainingConfig.getTrainingConfigsList();
    ScopedTrainingConfig.Builder deletedConfigsBuilder =
        ScopedTrainingConfig.newBuilder().setConfigScope(configScope);

    String context = getContextFromAnomalyConfigScope(configScope);
    ScopedTrainingConfig scopedTrainingConfig =
        getData(requestContext, context).orElse(ScopedTrainingConfig.getDefaultInstance());

    if (deleteAnomalyConfigOption.equals(
        DeleteAnomalyConfigOption.DELETE_ANOMALY_CONFIG_OPTION_WHOLE_TRAINING_CONFIG)) {
      upsertObject(
          requestContext,
          configHandler.deleteWholeTrainingConfigs(
              scopedTrainingConfig, deleteTrainingConfigFilters, deletedConfigsBuilder));
    }
    return deletedConfigsBuilder.build();
  }

  @Override
  public ScopedTrainingConfig updateScopedTrainingConfig(
      RequestContext requestContext, ScopedTrainingConfig scopedTrainingConfig) {
    Optional<ScopedTrainingConfig> currentConfig =
        getData(requestContext, getContextFromData(scopedTrainingConfig));
    ScopedTrainingConfig updatedScopedTrainingConfig =
        configHandler.merge(
            scopedTrainingConfig, currentConfig.orElse(ScopedTrainingConfig.getDefaultInstance()));

    return upsertObject(requestContext, updatedScopedTrainingConfig).getData();
  }

  private Map<String, ScopedTrainingConfig> fetchConfigMap(RequestContext requestContext) {
    return getAllObjects(requestContext).stream()
        .collect(Collectors.toMap(ContextualConfigObject::getContext, ConfigObject::getData));
  }

  private ScopedTrainingConfig filterConfigs(
      ScopedTrainingConfig scopedTrainingConfig,
      Set<TrainingConfig.TrainingConfigCase> configCases) {
    if (configCases.isEmpty()) {
      return scopedTrainingConfig;
    }

    ScopedTrainingConfig.Builder builder = ScopedTrainingConfig.newBuilder();
    builder.setConfigScope(scopedTrainingConfig.getConfigScope());
    List<TrainingConfig> trainingConfigs = scopedTrainingConfig.getTrainingConfigsList();
    List<TrainingConfig> filteredConfigs = new ArrayList<>();
    for (TrainingConfig.TrainingConfigCase configCase : configCases) {
      filteredConfigs.addAll(
          trainingConfigs.stream()
              .filter(trainingConfig -> trainingConfig.getTrainingConfigCase() == configCase)
              .collect(Collectors.toList()));
    }

    return builder.addAllTrainingConfigs(filteredConfigs).build();
  }

  private List<ScopedTrainingConfig> getResolvedConfigs(
      Map<String, ScopedTrainingConfig> configMap, String tenantId) {

    List<ScopedTrainingConfig> resolvedConfigs = new ArrayList<>();
    for (Map.Entry<String, ScopedTrainingConfig> entry : configMap.entrySet()) {

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
          ScopedTrainingConfig.newBuilder()
              .setConfigScope(
                  AnomalyConfigScope.newBuilder()
                      .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
                      .build())
              .addAllTrainingConfigs(getDefaultTrainingConfigs())
              .build());
    }
    return resolvedConfigs;
  }

  private ScopedTrainingConfig getResolvedConfig(
      Map<String, ScopedTrainingConfig> configMap,
      AnomalyConfigScope configScope,
      List<String> contextsWithIncreasingPriority) {
    ScopedTrainingConfig trainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(configScope)
            .addAllTrainingConfigs(getDefaultTrainingConfigs())
            .build();
    for (String context : contextsWithIncreasingPriority) {
      trainingConfig =
          configMap.containsKey(context)
              ? configHandler.merge(configMap.get(context), trainingConfig)
              : trainingConfig;
    }
    return trainingConfig;
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

  private List<TrainingConfig> getDefaultTrainingConfigs() {
    return Stream.concat(
            this.defaultApiNamingTrainingConfigs.stream(),
            this.defaultMetadataTrainingConfigs.stream())
        .collect(Collectors.toUnmodifiableList());
  }
}
