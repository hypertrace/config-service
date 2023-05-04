package ai.traceable.api.gateway.config.service.store;

import ai.traceable.api.gateway.config.service.filter.MetadataConfigFilterToPredicateConverter;
import ai.traceable.api.gateway.config.service.v1.ConfigMetadata;
import ai.traceable.api.gateway.config.service.v1.MetadataFilter;
import com.google.protobuf.Value;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class MetadataConfigStore
    extends IdentifiedObjectStoreWithFilter<ConfigMetadata, MetadataFilter> {
  private static final String CONFIG_METADATA_RESOURCE_NAME = "config-metadata";
  private static final String CONFIG_METADATA_RESOURCE_NAMESPACE = "api-gateway";
  private final Map<MetadataFilter.TypeCase, MetadataConfigFilterToPredicateConverter>
      filterConverterMap;

  @Inject
  public MetadataConfigStore(
      final ConfigServiceBlockingStub configServiceBlockingStub,
      final ConfigChangeEventGenerator configChangeEventGenerator,
      final Map<MetadataFilter.TypeCase, MetadataConfigFilterToPredicateConverter>
          filterConverterMap) {
    super(
        configServiceBlockingStub,
        CONFIG_METADATA_RESOURCE_NAMESPACE,
        CONFIG_METADATA_RESOURCE_NAME,
        configChangeEventGenerator);
    this.filterConverterMap = filterConverterMap;
  }

  @SneakyThrows
  @Override
  protected Optional<ConfigMetadata> buildDataFromValue(final Value value) {
    final ConfigMetadata.Builder metadataBuilder = ConfigMetadata.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, metadataBuilder);
    return Optional.of(metadataBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(final ConfigMetadata configMetadata) {
    return ConfigProtoConverter.convertToValue(configMetadata);
  }

  @Override
  protected String getContextFromData(final ConfigMetadata configMetadata) {
    return configMetadata.getOrgId();
  }

  @Override
  protected Optional<ConfigMetadata> filterConfigData(
      final ConfigMetadata configMetadata, final MetadataFilter filter) {
    final MetadataFilter.TypeCase filterCase = filter.getTypeCase();
    final MetadataConfigFilterToPredicateConverter converter = filterConverterMap.get(filterCase);

    if (converter == null) {
      log.error("Unhandled filter case: " + filterCase);
      return Optional.empty();
    }

    return Optional.of(configMetadata).filter(converter.convert(filter));
  }
}
