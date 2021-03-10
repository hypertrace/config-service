package ai.traceable.region.config.service.rules;

import ai.traceable.region.config.service.v1.RegionRule;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class RegionRuleConverter {

  public RegionRule convert(Value ruleConfig) throws InvalidProtocolBufferException {
    RegionRule.Builder builder = RegionRule.newBuilder();
    if (ruleConfig != null && ruleConfig.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    }
    return builder.build();
  }

  public Value convert(RegionRule regionRule) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(regionRule);
  }
}
