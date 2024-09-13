package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.v1.AnomalyConfidenceLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class ScopedGlobalConfigStatusChangeConverter {

  public Value convert(ScopedAnomalyConfigStatusChange config)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }

  public ScopedAnomalyConfigStatusChange convert(Value config)
      throws InvalidProtocolBufferException {
    ScopedAnomalyConfigStatusChange.Builder builder = ScopedAnomalyConfigStatusChange.newBuilder();

    if (config != null && config.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(config, builder);
    }
    return builder.build();
  }

  public ScopedAnomalyConfigStatus convertScopedConfig(
      ScopedAnomalyConfigStatusChange config,
      AnomalyConfidenceLevel defaultConfidenceLevel,
      AnomalyConfigStatus configStatus) {
    return ScopedAnomalyConfigStatus.newBuilder()
        .setConfigScope(config.getConfigScope())
        .setConfigStatus(configStatus)
        .setExcludedEventsConfig(config.getExcludedEventsConfig())
        .setMinConfidenceLevel(
            config.hasMinConfidenceLevel()
                ? config.getMinConfidenceLevel()
                : defaultConfidenceLevel)
        .setEnabledForExitSpans(config.getEnabledForExitSpans())
        .setModsecGlobalConfig(config.getModsecGlobalConfig())
        .build();
  }

  public ScopedAnomalyConfigStatusChange merge(
      ScopedAnomalyConfigStatusChange highPriorityConfig,
      ScopedAnomalyConfigStatusChange lowPriorityConfig) {
    return lowPriorityConfig.toBuilder().mergeFrom(highPriorityConfig).build();
  }

  public AnomalyConfigStatusChange merge(
      AnomalyConfigStatusChange highPriorityConfig, AnomalyConfigStatusChange lowPriorityConfig) {
    return lowPriorityConfig.toBuilder().mergeFrom(highPriorityConfig).build();
  }

  public AnomalyConfigStatus merge(
      AnomalyConfigStatusChange highPriorityConfig, AnomalyConfigStatus lowPriorityConfig) {
    AnomalyConfigStatus.Builder builder = lowPriorityConfig.toBuilder();
    if (highPriorityConfig.hasDisabled()) {
      builder.setDisabled(highPriorityConfig.getDisabled());
    }
    if (highPriorityConfig.hasInternal()) {
      builder.setInternal(highPriorityConfig.getInternal());
    }
    return builder.build();
  }
}
