package ai.traceable.attribute.resolution.config.service.v1.manager;

import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfigMetadata;
import ai.traceable.attribute.resolution.config.service.v1.CreateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsFilter;
import ai.traceable.attribute.resolution.config.service.v1.UpdateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.config.AttributeResolutionDefaultConfig;
import ai.traceable.attribute.resolution.config.service.v1.store.AttributeResolutionConfigStore;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AttributeResolutionConfigManagerImpl implements AttributeResolutionConfigManager {
  private final UuidGenerator uuidGenerator;
  private final AttributeResolutionConfigStore store;
  private final AttributeResolutionDefaultConfig defaultConfig;
  private final TimestampConverter timestampConverter;

  @Inject
  public AttributeResolutionConfigManagerImpl(
      UuidGenerator uuidGenerator,
      AttributeResolutionConfigStore store,
      AttributeResolutionDefaultConfig defaultConfig,
      TimestampConverter timestampConverter) {
    this.uuidGenerator = uuidGenerator;
    this.store = store;
    this.defaultConfig = defaultConfig;
    this.timestampConverter = timestampConverter;
  }

  @Override
  public List<AttributeResolutionConfig> getAttributeResolutionConfigs(
      RequestContext context, GetAttributeResolutionConfigsFilter filter) {
    List<AttributeResolutionConfig> attributeResolutionConfigs =
        store.getAllObjects(context, filter).stream()
            .map(
                configObject ->
                    configObject.getData().toBuilder()
                        .setMetadata(buildAttributeResolutionMetadata(configObject))
                        .build())
            .collect(Collectors.toUnmodifiableList());
    return mergeAttributeResolutionConfigs(filter, attributeResolutionConfigs);
  }

  @Override
  public AttributeResolutionConfig createAttributeResolutionConfig(
      RequestContext context, CreateAttributeResolutionConfigRequest request) {
    String id = uuidGenerator.generateId(UUID.randomUUID().toString());
    AttributeResolutionConfig config =
        AttributeResolutionConfig.newBuilder().setId(id).setData(request.getData()).build();
    return upsert(context, config);
  }

  @Override
  public AttributeResolutionConfig updateAttributeResolutionConfig(
      RequestContext context, UpdateAttributeResolutionConfigRequest request) {
    String id = request.getConfig().getId();
    AttributeResolutionConfig oldConfig =
        store
            .getData(context, id)
            .orElseGet(
                () ->
                    Optional.ofNullable(getDefaultConfigById(id))
                        .orElseThrow(
                            () ->
                                Status.NOT_FOUND
                                    .withDescription(
                                        String.format(
                                            "Unable to update as AttributeResolutionConfig with id = %s does not exist",
                                            id))
                                    .asRuntimeException()));
    AttributeResolutionConfig updated =
        oldConfig.toBuilder().setData(request.getConfig().getData()).build();
    return upsert(context, updated);
  }

  @Override
  public void deleteAttributeResolutionConfig(RequestContext context, String id) {
    if (isDefaultConfig(id)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Deleting default attribute resolution config is not allowed with ID : %s", id))
          .asRuntimeException();
    }
    store
        .deleteObject(context, id)
        .map(DeletedConfigObject::getDeletedData)
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        String.format(
                            "Attribute resolution config does not exist with ID : %s", id))
                    .asRuntimeException(context.buildTrailers()));
  }

  private AttributeResolutionConfig upsert(
      RequestContext context, AttributeResolutionConfig config) {
    ContextualConfigObject<AttributeResolutionConfig> configObject =
        store.upsertObject(context, config);
    return config.toBuilder().setMetadata(buildAttributeResolutionMetadata(configObject)).build();
  }

  private AttributeResolutionConfigMetadata.Builder buildAttributeResolutionMetadata(
      ContextualConfigObject<AttributeResolutionConfig> storedConfig) {
    return AttributeResolutionConfigMetadata.newBuilder()
        .setCreationTimestamp(timestampConverter.convert(storedConfig.getCreationTimestamp()))
        .setLastUpdatedTimestamp(
            timestampConverter.convert(storedConfig.getLastUpdatedTimestamp()));
  }

  private List<AttributeResolutionConfig> mergeAttributeResolutionConfigs(
      List<AttributeResolutionConfig> userConfigs) {
    Map<String, AttributeResolutionConfig> mergedConfigsMap =
        new LinkedHashMap<>(defaultConfig.getDefaultAttributeResolutionConfigMap());
    userConfigs.forEach(config -> mergedConfigsMap.put(config.getId(), config));
    return mergedConfigsMap.values().stream().collect(Collectors.toUnmodifiableList());
  }

  private List<AttributeResolutionConfig> mergeAttributeResolutionConfigs(
      GetAttributeResolutionConfigsFilter filter, List<AttributeResolutionConfig> userConfigs) {
    return mergeAttributeResolutionConfigs(userConfigs).stream()
        .filter(config -> applyFilter(filter, config))
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean applyFilter(
      GetAttributeResolutionConfigsFilter filter,
      AttributeResolutionConfig attributeResolutionConfig) {
    return Optional.of(attributeResolutionConfig)
        .filter(
            config -> !filter.hasEnabled() || config.getData().getEnabled() == filter.getEnabled())
        .filter(
            config ->
                !filter.hasEntityType()
                    || config.getData().getEntityType().equals(config.getData().getEntityType()))
        .isPresent();
  }

  private AttributeResolutionConfig getDefaultConfigById(String id) {
    return defaultConfig.getDefaultAttributeResolutionConfigMap().get(id);
  }

  private boolean isDefaultConfig(String id) {
    return defaultConfig.getDefaultAttributeResolutionConfigMap().containsKey(id);
  }
}
