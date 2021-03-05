package ai.traceable.ratelimiting.service;

import ai.traceable.ratelimiting.config.service.v1.RateLimitedEntity;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingRuleConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class RateLimitingConfigServiceUtils {
  public static Value toValue(Message message) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(message);
  }

  public static RateLimitingRuleConfig toRateLimitingRuleConfig(Value value)
      throws InvalidProtocolBufferException {
    RateLimitingRuleConfig.Builder builder = RateLimitingRuleConfig.newBuilder();
    if (value != null && value.getKindCase() != KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(value, builder);
    }
    return builder.build();
  }

  public static String getRuleRateLimitedEntityContext(RateLimitedEntity rateLimitedEntity) {
    return rateLimitedEntity.getEntityType().toString() + ":" + rateLimitedEntity.getEntityId();
  }
}
