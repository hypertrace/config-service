package ai.traceable.threatmanagement.config.service.eventscore;

import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import java.util.Optional;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class SecurityEventScoreContributionConverter {

  public Optional<SecurityEventScoreContribution> convert(Value ruleConfig)
      throws InvalidProtocolBufferException {
    if (ruleConfig == null
        || ruleConfig.getKindCase() == KindCase.NULL_VALUE
        || ruleConfig.getKindCase() == KindCase.KIND_NOT_SET) {
      return Optional.empty();
    }

    SecurityEventScoreContribution.Builder builder = SecurityEventScoreContribution.newBuilder();
    ConfigProtoConverter.mergeFromValue(ruleConfig, builder);
    return Optional.of(builder.build());
  }

  public Value convert(SecurityEventScoreContribution securityEventScoreContribution)
      throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(securityEventScoreContribution);
  }
}
