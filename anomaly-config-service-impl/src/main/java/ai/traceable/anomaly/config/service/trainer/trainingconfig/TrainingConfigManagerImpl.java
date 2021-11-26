package ai.traceable.anomaly.config.service.trainer.trainingconfig;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.trainer.GetTrainingConfigsFilter;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class TrainingConfigManagerImpl implements TrainingConfigManager {
  private final ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private final TrainingConfigConverter configConverter;

  @Inject
  public TrainingConfigManagerImpl(
      TrainingConfigConverter configConverter,
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    this.configConverter = configConverter;
    this.configServiceBlockingStub = configServiceBlockingStub;
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
      case SCOPE_NOT_SET: // for backward compatibility
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
        getResolvedConfig(configMap, contextsWithIncreasingPriority);

    Set<TrainingConfig.TrainingConfigCase> configCases = configConverter.convert(filter);

    return filterConfigs(trainingConfig, configCases);
  }

  @Override
  public List<ScopedTrainingConfig> getAllScopedTrainingConfig(
      RequestContext requestContext, GetTrainingConfigsFilter filter) {
    Map<String, ScopedTrainingConfig> trainingConfigMap = fetchConfigMap(requestContext);

    List<ScopedTrainingConfig> trainingConfigs =
        getResolvedConfigs(trainingConfigMap, requestContext.getTenantId().orElseThrow());

    Set<TrainingConfig.TrainingConfigCase> configCases = configConverter.convert(filter);

    return trainingConfigs.stream()
        .map(trainingConfig -> filterConfigs(trainingConfig, configCases))
        .collect(Collectors.toList());
  }

  @Override
  public ScopedTrainingConfig updateScopedTrainingConfig(
      RequestContext requestContext, ScopedTrainingConfig scopedTrainingConfig) {
    String context;

    AnomalyConfigScope configScope = scopedTrainingConfig.getConfigScope();

    switch (configScope.getScopeCase()) {
      case SCOPE_NOT_SET: // for backward compatibility
      case CUSTOMER_SCOPE:
        context =
            requestContext
                .getTenantId()
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Unable to get tenant id from request context"));
        break;
      case SERVICE_SCOPE:
        context = configScope.getServiceScope().getId();
        break;
      case API_SCOPE:
        context = configScope.getApiScope().getId();
        break;
      default:
        throw new RuntimeException(
            String.format("Invalid scope found: {%s}", configScope.getScopeCase()));
    }

    ScopedTrainingConfig changeToUpsert =
        configConverter.merge(
            scopedTrainingConfig,
            fetchConfig(requestContext, List.of(context))
                .orElse(ScopedTrainingConfig.getDefaultInstance()));

    return upsertConfig(requestContext, context, changeToUpsert);
  }

  private Optional<ScopedTrainingConfig> fetchConfig(
      RequestContext requestContext, List<String> contextsWithIncreasingPriority) {

    return fetchConfigValue(requestContext, contextsWithIncreasingPriority)
        .map(
            value -> {
              try {
                return configConverter.convert(value);
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private Optional<Value> fetchConfigValue(
      RequestContext requestContext, List<String> contextsWithIncreasingPriority) {
    try {
      GetConfigRequest getConfigRequest =
          GetConfigRequest.newBuilder()
              .addAllContexts(contextsWithIncreasingPriority)
              .setResourceNamespace(TrainingConfigConstants.TRAINING_CONFIG_NAMESPACE)
              .setResourceName(TrainingConfigConstants.TRAINING_CONFIG_RESOURCE_NAME)
              .build();

      Value value =
          requestContext.call(
              () -> configServiceBlockingStub.getConfig(getConfigRequest).getConfig());
      if (value != null && value.getKindCase() != Value.KindCase.KIND_NOT_SET) {
        return Optional.of(value);
      }
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw e;
    }
    return Optional.empty();
  }

  private Map<String, ScopedTrainingConfig> fetchConfigMap(RequestContext requestContext) {
    return fetchAllConfigValues(requestContext).stream()
        .collect(
            Collectors.toMap(
                ContextSpecificConfig::getContext,
                contextSpecificConfig -> {
                  try {
                    return configConverter.convert(contextSpecificConfig.getConfig());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                }));
  }

  private List<ContextSpecificConfig> fetchAllConfigValues(RequestContext requestContext) {
    try {
      GetAllConfigsRequest getAllConfigsRequest =
          GetAllConfigsRequest.newBuilder()
              .setResourceNamespace(TrainingConfigConstants.TRAINING_CONFIG_NAMESPACE)
              .setResourceName(TrainingConfigConstants.TRAINING_CONFIG_RESOURCE_NAME)
              .build();

      List<ContextSpecificConfig> contextSpecificConfigs =
          requestContext.call(
              () ->
                  configServiceBlockingStub
                      .getAllConfigs(getAllConfigsRequest)
                      .getContextSpecificConfigsList());

      return contextSpecificConfigs.stream()
          .filter(
              contextSpecificConfig ->
                  contextSpecificConfig.getConfig().getKindCase() != Value.KindCase.KIND_NOT_SET)
          .collect(Collectors.toList());

    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return List.of();
      }
      throw e;
    }
  }

  private ScopedTrainingConfig upsertConfig(
      RequestContext requestContext, String context, ScopedTrainingConfig config) {
    UpsertConfigRequest.Builder upsertConfigRequestBuilder;

    try {
      upsertConfigRequestBuilder =
          UpsertConfigRequest.newBuilder()
              .setContext(context)
              .setResourceNamespace(TrainingConfigConstants.TRAINING_CONFIG_NAMESPACE)
              .setResourceName(TrainingConfigConstants.TRAINING_CONFIG_RESOURCE_NAME)
              .setConfig(configConverter.convert(config));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to convert ScopedTrainingConfig {%s} to config object for context {%s}",
              config, context),
          e);
    }

    UpsertConfigResponse response;
    try {
      response =
          requestContext.call(
              () -> configServiceBlockingStub.upsertConfig(upsertConfigRequestBuilder.build()));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to update ScopedTrainingConfig {%s} for context {%s}", config, context),
          e);
    }

    try {
      return configConverter.convert(response.getConfig());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Unable to convert config response {%s} to ScopedTrainingConfig for context {%s}",
              response, context),
          e);
    }
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

      resolvedConfigs.add(getResolvedConfig(configMap, contextsWithIncreasingPriority));
    }

    return resolvedConfigs;
  }

  private ScopedTrainingConfig getResolvedConfig(
      Map<String, ScopedTrainingConfig> configMap, List<String> contextsWithIncreasingPriority) {
    ScopedTrainingConfig trainingConfig = ScopedTrainingConfig.getDefaultInstance();
    for (String context : contextsWithIncreasingPriority) {
      trainingConfig =
          configMap.containsKey(context)
              ? configConverter.merge(configMap.get(context), trainingConfig)
              : trainingConfig;
    }
    return trainingConfig;
  }
}
