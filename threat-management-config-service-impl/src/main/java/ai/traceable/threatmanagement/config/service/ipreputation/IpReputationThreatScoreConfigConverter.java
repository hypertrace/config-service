package ai.traceable.threatmanagement.config.service.ipreputation;

import ai.traceable.threatmanagement.config.service.v1.IpReputationThreatScoreConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class IpReputationThreatScoreConfigConverter {

  static IpReputationThreatScoreConfig convert(Value value) throws InvalidProtocolBufferException {
    IpReputationThreatScoreConfig.Builder builder = IpReputationThreatScoreConfig.newBuilder();

    if (value != null && value.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(value, builder);
    }
    return builder.build();
  }

  static Value convert(IpReputationThreatScoreConfig config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }
}
