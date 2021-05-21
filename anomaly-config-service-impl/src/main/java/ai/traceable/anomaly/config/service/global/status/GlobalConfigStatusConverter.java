package ai.traceable.anomaly.config.service.global.status;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class GlobalConfigStatusConverter {

  public AnomalyConfigStatus convert(Value ruleConfig, AnomalyConfigStatus defaultConfig)
      throws InvalidProtocolBufferException {

    AnomalyConfigStatusChange.Builder builder = AnomalyConfigStatusChange.newBuilder();
    if (ruleConfig != null && ruleConfig.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    }

    AnomalyConfigStatus.Builder resultBuilder = AnomalyConfigStatus.newBuilder();
    if (builder.hasDisabled()) {
      resultBuilder.setDisabled(builder.getDisabled());
    } else {
      resultBuilder.setDisabled(defaultConfig.getDisabled());
    }
    if (builder.hasInternal()) {
      resultBuilder.setInternal(builder.getInternal());
    } else {
      resultBuilder.setInternal(defaultConfig.getInternal());
    }

    return resultBuilder.build();
  }

  public AnomalyConfigStatusChange convert(Value ruleConfig) throws InvalidProtocolBufferException {
    AnomalyConfigStatusChange.Builder builder = AnomalyConfigStatusChange.newBuilder();
    if (ruleConfig != null && ruleConfig.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    }
    return builder.build();
  }

  public Value convert(AnomalyConfigStatusChange anomalyConfigStatusChange)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(anomalyConfigStatusChange);
  }
}
