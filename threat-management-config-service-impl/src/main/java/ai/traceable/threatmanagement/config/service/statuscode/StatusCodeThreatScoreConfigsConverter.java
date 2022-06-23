package ai.traceable.threatmanagement.config.service.statuscode;

import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfigs;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class StatusCodeThreatScoreConfigsConverter {

  static StatusCodeThreatScoreConfigs convert(Value value) throws InvalidProtocolBufferException {
    StatusCodeThreatScoreConfigs.Builder builder = StatusCodeThreatScoreConfigs.newBuilder();

    if (value != null && value.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(value, builder);
    }
    return builder.build();
  }

  static Value convert(StatusCodeThreatScoreConfigs config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }
}
