package ai.traceable.threatmanagement.config.service.statuscode;

import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfigs;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class StatusCodeThreatScoreConfigsConverter {

  static Optional<StatusCodeThreatScoreConfigs> convert(Value value)
      throws InvalidProtocolBufferException {
    if (value == null
        || value.getKindCase() == Value.KindCase.NULL_VALUE
        || value.getKindCase() == Value.KindCase.KIND_NOT_SET) {
      return Optional.empty();
    }

    StatusCodeThreatScoreConfigs.Builder builder = StatusCodeThreatScoreConfigs.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  static Value convert(StatusCodeThreatScoreConfigs config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }
}
