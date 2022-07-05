package ai.traceable.anomaly.config.service.trainer.trainingaction;

import static ai.traceable.anomaly.config.service.trainer.trainingaction.TrainingActionConstants.TRAINING_ACTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.anomaly.config.service.trainer.trainingaction.TrainingActionConstants.TRAINING_ACTION_NAMESPACE;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class TrainingActionManagerImpl extends IdentifiedObjectStore<ScopedTrainingActionConfig>
    implements TrainingActionManager {
  private final TrainingActionConverter actionConverter;

  @Inject
  TrainingActionManagerImpl(
      TrainingActionConverter actionConverter,
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub, TRAINING_ACTION_NAMESPACE, TRAINING_ACTION_CONFIG_RESOURCE_NAME);
    this.actionConverter = actionConverter;
  }

  @Override
  public ScopedTrainingActionConfig upsertTrainingAction(
      RequestContext requestContext,
      AnomalyConfigScope configScope,
      TrainingAction trainingAction) {
    ScopedTrainingActionConfig existingScopedActionConfig =
        getData(requestContext, getContextFromAnomalyConfigScope(configScope, requestContext))
            .orElse(ScopedTrainingActionConfig.getDefaultInstance());
    ScopedTrainingActionConfig updatedScopedTrainingConfig =
        actionConverter.merge(configScope, trainingAction, existingScopedActionConfig);

    return upsertObject(requestContext, updatedScopedTrainingConfig).getData();
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
  public void deleteTrainingAction(RequestContext requestContext, AnomalyConfigScope configScope) {
    deleteObject(requestContext, getContextFromAnomalyConfigScope(configScope, requestContext));
  }

  private List<ScopedTrainingActionConfig> getResolvedScopedActionConfigs(
      Map<String, ScopedTrainingActionConfig> contextToScopedActionConfigMap, String tenantId) {
    List<ScopedTrainingActionConfig> resolvedScopedActionConfigs = new ArrayList<>();
    for (Map.Entry<String, ScopedTrainingActionConfig> entry :
        contextToScopedActionConfigMap.entrySet()) {
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

      resolvedScopedActionConfigs.add(
          getResolvedScopedActionConfig(
              contextToScopedActionConfigMap, contextsWithIncreasingPriority));
    }

    return resolvedScopedActionConfigs;
  }

  private ScopedTrainingActionConfig getResolvedScopedActionConfig(
      Map<String, ScopedTrainingActionConfig> contextToScopedActionConfigMap,
      List<String> contextsWithIncreasingPriority) {
    ScopedTrainingActionConfig scopedActionConfig = ScopedTrainingActionConfig.getDefaultInstance();
    for (String context : contextsWithIncreasingPriority) {
      scopedActionConfig =
          contextToScopedActionConfigMap.containsKey(context)
              ? actionConverter.timeBasedMerge(
                  contextToScopedActionConfigMap.get(context), scopedActionConfig)
              : scopedActionConfig;
    }
    return scopedActionConfig;
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

  @Override
  @SneakyThrows
  protected Value buildValueFromData(ScopedTrainingActionConfig data) {
    return actionConverter.convert(data);
  }

  @Override
  protected String getContextFromData(ScopedTrainingActionConfig data) {
    return getContextFromAnomalyConfigScope(data.getConfigScope(), RequestContext.CURRENT.get());
  }

  private String getContextFromAnomalyConfigScope(
      AnomalyConfigScope configScope, RequestContext requestContext) {
    String context;
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
    return context;
  }
}
