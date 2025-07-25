package ai.traceable.customsignature.config.service.rules;

import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class CustomSignatureRuleConverter {

  public CustomSignatureRule convert(Value ruleConfig) throws InvalidProtocolBufferException {
    CustomSignatureRule.Builder builder = CustomSignatureRule.newBuilder();
    if (ruleConfig != null && ruleConfig.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    }
    return builder.build();
  }

  public Value convert(CustomSignatureRule customSignatureRule)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(customSignatureRule);
  }
}
