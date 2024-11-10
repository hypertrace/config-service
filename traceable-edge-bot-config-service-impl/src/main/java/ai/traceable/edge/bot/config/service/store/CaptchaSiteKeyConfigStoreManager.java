package ai.traceable.edge.bot.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfig;
import ai.traceable.edge.bot.config.service.v1.DeleteRequest;
import ai.traceable.edge.bot.config.service.v1.DeleteResponse;
import ai.traceable.edge.bot.config.service.v1.GetAllRequest;
import ai.traceable.edge.bot.config.service.v1.GetAllResponse;
import ai.traceable.edge.bot.config.service.v1.GetRequest;
import ai.traceable.edge.bot.config.service.v1.GetResponse;
import ai.traceable.edge.bot.config.service.v1.UpsertRequest;
import ai.traceable.edge.bot.config.service.v1.UpsertResponse;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class CaptchaSiteKeyConfigStoreManager {
  private final UuidGenerator uuidGenerator;
  private final CaptchaSiteKeyConfigStore captchaSiteKeyConfigStore;

  @Inject
  public CaptchaSiteKeyConfigStoreManager(
      UuidGenerator uuidGenerator, CaptchaSiteKeyConfigStore captchaSiteKeyConfigStore) {
    this.uuidGenerator = uuidGenerator;
    this.captchaSiteKeyConfigStore = captchaSiteKeyConfigStore;
  }

  public GetAllResponse getAll(RequestContext requestContext, GetAllRequest request) {
    List<CaptchaSiteKeyConfig> configs = captchaSiteKeyConfigStore.getAllConfigData(requestContext);
    return GetAllResponse.newBuilder().addAllConfigs(configs).build();
  }

  public GetResponse get(RequestContext requestContext, GetRequest request) {
    Optional<CaptchaSiteKeyConfig> config =
        captchaSiteKeyConfigStore.getData(requestContext, request.getId());
    return config
        .map(
            captchaSiteKeyConfig ->
                GetResponse.newBuilder().setConfig(captchaSiteKeyConfig).build())
        .orElseGet(() -> GetResponse.newBuilder().build());
  }

  public UpsertResponse upsert(RequestContext requestContext, UpsertRequest request) {
    var config = request.getConfig();
    if (config.getId().isEmpty()) {
      config = config.toBuilder().setId(uuidGenerator.generateRandomId()).build();
    }
    ContextualConfigObject<CaptchaSiteKeyConfig> configObject =
        captchaSiteKeyConfigStore.upsertObject(requestContext, config);
    return UpsertResponse.newBuilder().setConfig(configObject.getData()).build();
  }

  public DeleteResponse delete(RequestContext requestContext, DeleteRequest request) {
    var deleted = captchaSiteKeyConfigStore.deleteObject(requestContext, request.getId());
    if (deleted.isPresent() && deleted.get().getDeletedData().isPresent()) {
      return DeleteResponse.newBuilder()
          .setDeletedConfig(deleted.get().getDeletedData().get())
          .build();
    } else {
      return DeleteResponse.newBuilder().build();
    }
  }
}
