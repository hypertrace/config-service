package ai.traceable.risk.config.service.processor;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class RiskConfigConverter<M extends Message> {

  public Value convert(M config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }

  public M convert(Value ruleConfig, M.Builder builder) throws InvalidProtocolBufferException {
    if (ruleConfig != null
        && ruleConfig.getKindCase() != Value.KindCase.NULL_VALUE
        && ruleConfig.getKindCase() != Value.KindCase.KIND_NOT_SET) {
      ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    }
    return (M) builder.build();
  }
}
