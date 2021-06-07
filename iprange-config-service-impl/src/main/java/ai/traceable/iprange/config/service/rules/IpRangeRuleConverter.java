package ai.traceable.iprange.config.service.rules;

import ai.traceable.iprange.config.service.v1.IpRangeRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class IpRangeRuleConverter {
  IpRangeRule convert(Value ruleConfig) throws InvalidProtocolBufferException {
    IpRangeRule.Builder builder = IpRangeRule.newBuilder();
    if (ruleConfig != null && ruleConfig.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    }
    return builder.build();
  }

  Value convert(IpRangeRule ipRangeRule) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(ipRangeRule);
  }
}
