package ai.traceable.threatmanagement.config.service.eventtype;

import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import java.util.Optional;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class SecurityEventTypeContributionConverter {

  public Optional<SecurityEventTypeContribution> convert(Value ruleConfig)
      throws InvalidProtocolBufferException {
    if (ruleConfig == null
        || ruleConfig.getKindCase() == KindCase.NULL_VALUE
        || ruleConfig.getKindCase() == KindCase.KIND_NOT_SET) {
      return Optional.empty();
    }

    SecurityEventTypeContribution.Builder builder = SecurityEventTypeContribution.newBuilder();
    ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    return Optional.of(builder.build());
  }

  public Value convert(SecurityEventTypeContribution securityEventTypeContribution)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(securityEventTypeContribution);
  }
}
