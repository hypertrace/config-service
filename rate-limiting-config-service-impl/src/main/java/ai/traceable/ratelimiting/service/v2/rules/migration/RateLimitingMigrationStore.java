package ai.traceable.ratelimiting.service.v2.rules.migration;

import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_MIGRATION_CONFIG_RESOURCE_NAME;
import static ai.traceable.ratelimiting.service.v2.constants.RateLimitingConfigConstants.RATE_LIMITING_RULE_CONFIG_RESOURCE_NAMESPACE;

import ai.traceable.ratelimiting.config.service.v2.RateLimitingMigrationConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class RateLimitingMigrationStore extends DefaultObjectStore<RateLimitingMigrationConfig> {

  @Inject
  public RateLimitingMigrationStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        RATE_LIMITING_MIGRATION_CONFIG_RESOURCE_NAME,
        RATE_LIMITING_RULE_CONFIG_RESOURCE_NAMESPACE,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<RateLimitingMigrationConfig> buildDataFromValue(Value value) {
    try {
      RateLimitingMigrationConfig.Builder builder = RateLimitingMigrationConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize RateLimitingMigrationConfig from value : {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(RateLimitingMigrationConfig rateLimitingMigrationConfig) {
    return ConfigProtoConverter.convertToValue(rateLimitingMigrationConfig);
  }
}
