package ai.traceable.anomaly.config.service.trainer.trainingaction;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingActionConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingAction;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingActionConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.time.Clock;
import java.util.EnumMap;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class TrainingActionConverter {
  private final Clock clock;

  @Inject
  public TrainingActionConverter(Clock clock) {
    this.clock = clock;
  }

  public ScopedTrainingActionConfig convert(Value config) throws InvalidProtocolBufferException {
    ScopedTrainingActionConfig.Builder builder = ScopedTrainingActionConfig.newBuilder();

    if (config != null && config.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(config, builder);
    }
    return builder.build();
  }

  public Value convert(ScopedTrainingActionConfig scopedActionConfig)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(scopedActionConfig);
  }

  public ScopedTrainingActionConfig timeBasedMerge(
      ScopedTrainingActionConfig preferredScopedActionConfig,
      ScopedTrainingActionConfig fallbackScopedActionConfig) {
    if (preferredScopedActionConfig.equals(ScopedTrainingActionConfig.getDefaultInstance())) {
      return fallbackScopedActionConfig;
    }
    if (fallbackScopedActionConfig.equals(ScopedTrainingActionConfig.getDefaultInstance())) {
      return preferredScopedActionConfig;
    }

    EnumMap<TrainingAction.ActionCase, TrainingActionConfig> trainingActionConfigMap =
        new EnumMap<>(TrainingAction.ActionCase.class);

    preferredScopedActionConfig
        .getTrainingActionConfigList()
        .forEach(
            trainingActionConfig ->
                trainingActionConfigMap.put(
                    trainingActionConfig.getTrainingAction().getActionCase(),
                    trainingActionConfig));

    fallbackScopedActionConfig
        .getTrainingActionConfigList()
        .forEach(
            trainingActionConfig ->
                timeBasedMergeActionConfigWithMap(trainingActionConfig, trainingActionConfigMap));

    return ScopedTrainingActionConfig.newBuilder()
        .setConfigScope(preferredScopedActionConfig.getConfigScope())
        .addAllTrainingActionConfig(trainingActionConfigMap.values())
        .build();
  }

  public ScopedTrainingActionConfig merge(
      AnomalyConfigScope anomalyConfigScope,
      TrainingAction inputTrainingAction,
      ScopedTrainingActionConfig existingScopedActionConfig) {
    EnumMap<TrainingAction.ActionCase, TrainingActionConfig> actionToTrainingActionConfigMap =
        new EnumMap<>(TrainingAction.ActionCase.class);

    existingScopedActionConfig
        .getTrainingActionConfigList()
        .forEach(
            trainingActionConfig ->
                actionToTrainingActionConfigMap.put(
                    trainingActionConfig.getTrainingAction().getActionCase(),
                    trainingActionConfig));

    mergeActionWithMap(inputTrainingAction, actionToTrainingActionConfigMap);

    return ScopedTrainingActionConfig.newBuilder()
        .setConfigScope(anomalyConfigScope)
        .addAllTrainingActionConfig(actionToTrainingActionConfigMap.values())
        .build();
  }

  private void timeBasedMergeActionConfigWithMap(
      TrainingActionConfig inputActionConfig,
      EnumMap<TrainingAction.ActionCase, TrainingActionConfig> actionToTrainingActionConfigMap) {
    TrainingAction.ActionCase actionCase = inputActionConfig.getTrainingAction().getActionCase();
    if (actionToTrainingActionConfigMap.containsKey(actionCase)) {
      TrainingActionConfig actionConfig = actionToTrainingActionConfigMap.get(actionCase);
      actionToTrainingActionConfigMap.put(
          actionCase, timeBasedMergeActionConfigs(inputActionConfig, actionConfig));
    } else {
      actionToTrainingActionConfigMap.put(actionCase, inputActionConfig);
    }
  }

  private void mergeActionWithMap(
      TrainingAction inputTrainingAction,
      EnumMap<TrainingAction.ActionCase, TrainingActionConfig> actionToTrainingActionConfigMap) {
    TrainingAction.ActionCase actionCase = inputTrainingAction.getActionCase();
    TrainingAction mergedAction;
    if (actionToTrainingActionConfigMap.containsKey(actionCase)) {
      TrainingActionConfig actionConfig = actionToTrainingActionConfigMap.get(actionCase);
      mergedAction =
          actionConfig.getTrainingAction().toBuilder().mergeFrom(inputTrainingAction).build();
    } else {
      mergedAction = inputTrainingAction;
    }
    TrainingActionConfig mergedTrainingActionConfig =
        TrainingActionConfig.newBuilder()
            .setTrainingAction(mergedAction)
            .setTimestamp(clock.millis())
            .build();
    actionToTrainingActionConfigMap.put(actionCase, mergedTrainingActionConfig);
  }

  private TrainingActionConfig timeBasedMergeActionConfigs(
      TrainingActionConfig actionConfig1, TrainingActionConfig actionConfig2) {
    TrainingAction mergedAction;
    long mergedTimestamp;
    if (actionConfig1.getTimestamp() > actionConfig2.getTimestamp()) {
      mergedAction =
          actionConfig2.getTrainingAction().toBuilder()
              .mergeFrom(actionConfig1.getTrainingAction())
              .build();
      mergedTimestamp = actionConfig1.getTimestamp();
    } else {
      mergedAction =
          actionConfig1.getTrainingAction().toBuilder()
              .mergeFrom(actionConfig2.getTrainingAction())
              .build();
      mergedTimestamp = actionConfig2.getTimestamp();
    }
    return TrainingActionConfig.newBuilder()
        .setTrainingAction(mergedAction)
        .setTimestamp(mergedTimestamp)
        .build();
  }
}
