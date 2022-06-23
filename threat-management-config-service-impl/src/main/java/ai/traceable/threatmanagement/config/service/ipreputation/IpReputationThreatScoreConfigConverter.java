package ai.traceable.threatmanagement.config.service.ipreputation;

import ai.traceable.threatmanagement.config.service.v1.IpReputationThreatScoreConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class IpReputationThreatScoreConfigConverter {

  static Optional<IpReputationThreatScoreConfig> convert(Value value)
      throws InvalidProtocolBufferException {
    if (value == null
        || value.getKindCase() == Value.KindCase.NULL_VALUE
        || value.getKindCase() == Value.KindCase.KIND_NOT_SET) {
      return Optional.empty();
    }

    IpReputationThreatScoreConfig.Builder builder = IpReputationThreatScoreConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  static Value convert(IpReputationThreatScoreConfig config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }
}
