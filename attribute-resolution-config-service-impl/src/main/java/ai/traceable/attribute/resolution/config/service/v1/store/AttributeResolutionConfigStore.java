package ai.traceable.attribute.resolution.config.service.v1.store;

import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfigData;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsFilter;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

@Slf4j
public class AttributeResolutionConfigStore
    extends IdentifiedObjectStoreWithFilter<
        AttributeResolutionConfig, GetAttributeResolutionConfigsFilter> {

  private static final String NAMESPACE = "attribute-resolution-config";
  private static final String RESOURCE_NAME = "attribute-resolution-config-resource";

  @Inject
  public AttributeResolutionConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator changeEventGenerator) {
    super(configServiceBlockingStub, NAMESPACE, RESOURCE_NAME, changeEventGenerator);
  }

  @Override
  protected Optional<AttributeResolutionConfig> filterConfigData(
      AttributeResolutionConfig resolutionConfig, GetAttributeResolutionConfigsFilter filter) {
    return Optional.of(resolutionConfig)
        .filter(config -> filterConfigData(config.getData(), filter));
  }

  private boolean filterConfigData(
      AttributeResolutionConfigData configData, GetAttributeResolutionConfigsFilter filter) {
    return Optional.of(configData)
        .filter(data -> !filter.hasEnabled() || filter.getEnabled() == data.getEnabled())
        .filter(
            data -> !filter.hasEntityType() || filter.getEntityType().equals(data.getEntityType()))
        .isPresent();
  }

  @Override
  protected Optional<AttributeResolutionConfig> buildDataFromValue(Value value) {
    try {
      AttributeResolutionConfig.Builder builder = AttributeResolutionConfig.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      log.error("Failed to parse AttributeResolutionConfig from value {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(AttributeResolutionConfig data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(AttributeResolutionConfig data) {
    return data.getId();
  }
}
