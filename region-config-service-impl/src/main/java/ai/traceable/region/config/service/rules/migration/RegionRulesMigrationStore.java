package ai.traceable.region.config.service.rules.migration;

import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_CONFIG_NAMESPACE;
import static ai.traceable.region.config.service.constants.RegionConfigConstants.REGION_RULE_MIGRATION_CONFIG_RESOURCE_NAME;

import ai.traceable.region.config.service.v1.RegionRuleMigrationConfig;
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
public class RegionRulesMigrationStore extends DefaultObjectStore<RegionRuleMigrationConfig> {

  @Inject
  public RegionRulesMigrationStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        REGION_RULE_CONFIG_NAMESPACE,
        REGION_RULE_MIGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<RegionRuleMigrationConfig> buildDataFromValue(Value value) {
    try {
      RegionRuleMigrationConfig.Builder builder = RegionRuleMigrationConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Could not deserialize RegionRuleMigrationConfig from value : {}", value, e);
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(RegionRuleMigrationConfig regionRuleMigrationConfig) {
    return ConfigProtoConverter.convertToValue(regionRuleMigrationConfig);
  }
}
