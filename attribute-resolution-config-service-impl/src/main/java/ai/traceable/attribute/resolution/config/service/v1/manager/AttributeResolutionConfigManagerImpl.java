package ai.traceable.attribute.resolution.config.service.v1.manager;

import ai.traceable.attribute.resolution.config.service.v1.AttributeResolutionConfig;
import ai.traceable.attribute.resolution.config.service.v1.CreateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.GetAttributeResolutionConfigsFilter;
import ai.traceable.attribute.resolution.config.service.v1.UpdateAttributeResolutionConfigRequest;
import ai.traceable.attribute.resolution.config.service.v1.store.AttributeResolutionConfigStore;
import ai.traceable.config.utils.UuidGenerator;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class AttributeResolutionConfigManagerImpl implements AttributeResolutionConfigManager {
  private final UuidGenerator uuidGenerator;
  private final AttributeResolutionConfigStore store;

  @Inject
  public AttributeResolutionConfigManagerImpl(
      UuidGenerator uuidGenerator, AttributeResolutionConfigStore store) {
    this.uuidGenerator = uuidGenerator;
    this.store = store;
  }

  @Override
  public List<AttributeResolutionConfig> getAttributeResolutionConfigs(
      RequestContext context, GetAttributeResolutionConfigsFilter filter) {
    return store.getAllConfigData(context, filter);
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
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription(
                            String.format(
                                "Attribute resolution config does not exist with ID : %s", id))
                        .asRuntimeException());
    AttributeResolutionConfig updated =
        oldConfig.toBuilder().setData(request.getConfig().getData()).build();
    return upsert(context, updated);
  }

  @Override
  public void deleteAttributeResolutionConfig(RequestContext context, String id) {
    store
        .deleteObject(context, id)
        .map(DeletedConfigObject::getDeletedData)
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription(
                        String.format(
                            "Attribute resolution config does not exist with ID : {}", id))
                    .asRuntimeException(context.buildTrailers()));
  }

  private AttributeResolutionConfig upsert(
      RequestContext context, AttributeResolutionConfig config) {
    return store.upsertObject(context, config).getData();
  }
}
