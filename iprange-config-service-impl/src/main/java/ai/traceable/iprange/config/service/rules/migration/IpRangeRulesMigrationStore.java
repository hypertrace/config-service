package ai.traceable.iprange.config.service.rules.migration;

import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_CONFIG_NAMESPACE;
import static ai.traceable.iprange.config.service.constants.IpRangeConfigConstants.IPRANGE_RULE_MIGRATION_CONFIG_RESOURCE_NAME;

import ai.traceable.iprange.config.service.v1.IpRangeMigrationConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class IpRangeRulesMigrationStore extends DefaultObjectStore<IpRangeMigrationConfig> {

  @Inject
  public IpRangeRulesMigrationStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        IPRANGE_RULE_CONFIG_NAMESPACE,
        IPRANGE_RULE_MIGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<IpRangeMigrationConfig> buildDataFromValue(Value value) {
    try {
      IpRangeMigrationConfig.Builder builder = IpRangeMigrationConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize IpRangeMigrationConfig from value : {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(IpRangeMigrationConfig ipRangeMigrationConfig) {
    return ConfigProtoConverter.convertToValue(ipRangeMigrationConfig);
  }
}
