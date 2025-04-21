package ai.traceable.genai.config.service.v1.store;

import ai.traceable.genai.config.service.v1.GenAiConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class GenAiConfigConverter {

  public Value convert(GenAiConfig config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }

  public GenAiConfig convert(Value ruleConfig, GenAiConfig.Builder builder)
      throws InvalidProtocolBufferException {
    if (ruleConfig != null
        && !ruleConfig.getKindCase().equals(Value.KindCase.NULL_VALUE)
        && !ruleConfig.getKindCase().equals(Value.KindCase.KIND_NOT_SET)) {
      ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    }
    return builder.build();
  }
}
