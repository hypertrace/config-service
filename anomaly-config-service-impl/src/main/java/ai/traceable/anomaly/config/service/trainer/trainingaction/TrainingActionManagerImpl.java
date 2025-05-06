package ai.traceable.anomaly.config.service.trainer.trainingaction;

import static ai.traceable.anomaly.config.service.trainer.trainingaction.TrainingActionConstants.TRAINING_ACTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.anomaly.config.service.trainer.trainingaction.TrainingActionConstants.TRAINING_ACTION_NAMESPACE;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.trainer.TrainerConfigServiceConfig;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction;
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
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class TrainingActionManagerImpl extends IdentifiedObjectStore<ScopedTrainingActionConfig>
    implements TrainingActionManager {
  private final TrainingActionConverter actionConverter;
  private final AnomalyConfigScopeUtils anomalyConfigScopeUtils;
  private final TrainerConfigServiceConfig config;

  @Inject
  TrainingActionManagerImpl(
      TrainingActionConverter actionConverter,
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      AnomalyConfigScopeUtils anomalyConfigScopeUtils,
      TrainerConfigServiceConfig config,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        TRAINING_ACTION_NAMESPACE,
        TRAINING_ACTION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.actionConverter = actionConverter;
    this.anomalyConfigScopeUtils = anomalyConfigScopeUtils;
    this.config = config;
  }

  @Override
  public ScopedTrainingActionConfig upsertTrainingAction(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      TrainingAction trainingAction) {
    ScopedTrainingActionConfig existingScopedActionConfig =
        getData(
                requestContext,
                anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(configScope))
            .orElse(ScopedTrainingActionConfig.getDefaultInstance());
    ScopedTrainingActionConfig updatedScopedTrainingConfig =
        actionConverter.merge(configScope, trainingAction, existingScopedActionConfig);

    return mergeWithDefaults(upsertObject(requestContext, updatedScopedTrainingConfig).getData());
  }

  @Override
  public List<ScopedTrainingActionConfig> getAllTrainingActions(RequestContext requestContext) {
    Map<String, ScopedTrainingActionConfig> contextToScopedActionConfigMap =
        getAllObjects(requestContext).stream()
            .collect(
                Collectors.toMap(
                    ContextualConfigObject::getContext, ContextualConfigObject::getData));

    return getResolvedScopedActionConfigs(
        contextToScopedActionConfigMap,
        requestContext
            .getTenantId()
            .orElseThrow(
                () ->
                    new IllegalArgumentException("Unable to get tenant id from request context")));
  }

  @Override
  public ScopedTrainingActionConfig getTrainingAction(
      RequestContext requestContext, AnomalyConfigScope configScope) {
    Map<String, ScopedTrainingActionConfig> contextToScopedActionConfigMap =
        getAllObjects(requestContext).stream()
            .collect(
                Collectors.toMap(
                    ContextualConfigObject::getContext, ContextualConfigObject::getData));
    List<String> contextsWithIncreasingPriority =
        anomalyConfigScopeUtils.getContextsWithIncreasingPriority(
            requestContext
                .getTenantId()
                .orElseThrow(() -> new IllegalArgumentException("Unable to get tenant ID")),
            configScope);
    return getResolvedScopedActionConfig(
        contextToScopedActionConfigMap, contextsWithIncreasingPriority, configScope);
  }

  @Override
  public void deleteTrainingAction(RequestContext requestContext, AnomalyConfigScope configScope) {
    deleteObject(
        requestContext, anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(configScope));
  }

  private List<ScopedTrainingActionConfig> getResolvedScopedActionConfigs(
      Map<String, ScopedTrainingActionConfig> contextToScopedActionConfigMap, String tenantId) {
    List<ScopedTrainingActionConfig> resolvedScopedActionConfigs = new ArrayList<>();
    for (Map.Entry<String, ScopedTrainingActionConfig> entry :
        contextToScopedActionConfigMap.entrySet()) {
      AnomalyConfigScope anomalyConfigScope = entry.getValue().getConfigScope();
      List<String> contextsWithIncreasingPriority =
          anomalyConfigScopeUtils.getContextsWithIncreasingPriority(tenantId, anomalyConfigScope);
      resolvedScopedActionConfigs.add(
          getResolvedScopedActionConfig(
              contextToScopedActionConfigMap, contextsWithIncreasingPriority, anomalyConfigScope));
    }

    if (!contextToScopedActionConfigMap.containsKey(tenantId)) {
      resolvedScopedActionConfigs.add(
          ScopedTrainingActionConfig.newBuilder()
              .setConfigScope(
                  AnomalyConfigScope.newBuilder()
                      .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
                      .build())
              .addAllTrainingActionConfig(config.getDefaultTrainingActionConfigs())
              .build());
    }
    return resolvedScopedActionConfigs;
  }

  private ScopedTrainingActionConfig getResolvedScopedActionConfig(
      Map<String, ScopedTrainingActionConfig> contextToScopedActionConfigMap,
      List<String> contextsWithIncreasingPriority,
      AnomalyConfigScope configScope) {
    ScopedTrainingActionConfig scopedActionConfig =
        ScopedTrainingActionConfig.newBuilder().setConfigScope(configScope).build();
    for (String context : contextsWithIncreasingPriority) {
      scopedActionConfig =
          contextToScopedActionConfigMap.containsKey(context)
              ? actionConverter.timeBasedMerge(
                  contextToScopedActionConfigMap.get(context), scopedActionConfig)
              : scopedActionConfig;
    }
    return mergeWithDefaults(scopedActionConfig);
  }

  @Override
  protected Optional<ScopedTrainingActionConfig> buildDataFromValue(Value value) {
    try {
      return Optional.of(actionConverter.convert(value));
    } catch (InvalidProtocolBufferException exception) {
      log.error("Unable to convert config to ScopedTrainingActionConfig for value: {}", value);
      return Optional.empty();
    }
  }

  private ScopedTrainingActionConfig mergeWithDefaults(ScopedTrainingActionConfig existingConfig) {
    ScopedTrainingActionConfig.Builder builder =
        ScopedTrainingActionConfig.newBuilder(existingConfig);

    Set<TrainingAction.ActionCase> existingActionTypes =
        existingConfig.getTrainingActionConfigList().stream()
            .map(trainingActionConfig -> trainingActionConfig.getTrainingAction().getActionCase())
            .collect(Collectors.toSet());

    config.getDefaultTrainingActionConfigs().stream()
        .filter(
            defaultAction ->
                !existingActionTypes.contains(defaultAction.getTrainingAction().getActionCase()))
        .forEach(builder::addTrainingActionConfig);
    return builder.build();
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(ScopedTrainingActionConfig data) {
    return actionConverter.convert(data);
  }

  @Override
  protected String getContextFromData(ScopedTrainingActionConfig data) {
    return anomalyConfigScopeUtils.getContextFromAnomalyConfigScope(data.getConfigScope());
  }
}
