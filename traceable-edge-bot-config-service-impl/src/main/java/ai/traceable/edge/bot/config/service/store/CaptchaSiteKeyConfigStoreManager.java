package ai.traceable.edge.bot.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.bot.config.service.v1.CaptchaSiteKeyConfig;
import ai.traceable.edge.bot.config.service.v1.DeleteCaptchaSiteKeyConfigRequest;
import ai.traceable.edge.bot.config.service.v1.DeleteCaptchaSiteKeyConfigResponse;
import ai.traceable.edge.bot.config.service.v1.GetAllCaptchaSiteKeyConfigsRequest;
import ai.traceable.edge.bot.config.service.v1.GetAllCaptchaSiteKeyConfigsResponse;
import ai.traceable.edge.bot.config.service.v1.GetCaptchaSiteKeyConfigRequest;
import ai.traceable.edge.bot.config.service.v1.GetCaptchaSiteKeyConfigResponse;
import ai.traceable.edge.bot.config.service.v1.UpsertCaptchaSiteKeyConfigRequest;
import ai.traceable.edge.bot.config.service.v1.UpsertCaptchaSiteKeyConfigResponse;
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

  public GetAllCaptchaSiteKeyConfigsResponse getAll(
      RequestContext requestContext, GetAllCaptchaSiteKeyConfigsRequest request) {
    List<CaptchaSiteKeyConfig> configs = captchaSiteKeyConfigStore.getAllConfigData(requestContext);
    return GetAllCaptchaSiteKeyConfigsResponse.newBuilder().addAllConfigs(configs).build();
  }

  public GetCaptchaSiteKeyConfigResponse get(
      RequestContext requestContext, GetCaptchaSiteKeyConfigRequest request) {
    Optional<CaptchaSiteKeyConfig> config =
        captchaSiteKeyConfigStore.getData(requestContext, request.getId());
    return config
        .map(
            captchaSiteKeyConfig ->
                GetCaptchaSiteKeyConfigResponse.newBuilder()
                    .setConfig(captchaSiteKeyConfig)
                    .build())
        .orElseGet(GetCaptchaSiteKeyConfigResponse::getDefaultInstance);
  }

  public UpsertCaptchaSiteKeyConfigResponse upsert(
      RequestContext requestContext, UpsertCaptchaSiteKeyConfigRequest request) {
    var config = request.getConfig();
    if (config.getId().isEmpty()) {
      config = config.toBuilder().setId(uuidGenerator.generateRandomId()).build();
    }
    ContextualConfigObject<CaptchaSiteKeyConfig> configObject =
        captchaSiteKeyConfigStore.upsertObject(requestContext, config);
    return UpsertCaptchaSiteKeyConfigResponse.newBuilder()
        .setConfig(configObject.getData())
        .build();
  }

  public DeleteCaptchaSiteKeyConfigResponse delete(
      RequestContext requestContext, DeleteCaptchaSiteKeyConfigRequest request) {
    var deleted = captchaSiteKeyConfigStore.deleteObject(requestContext, request.getId());
    if (deleted.isPresent() && deleted.get().getDeletedData().isPresent()) {
      return DeleteCaptchaSiteKeyConfigResponse.newBuilder()
          .setDeletedConfig(deleted.get().getDeletedData().get())
          .build();
    } else {
      return DeleteCaptchaSiteKeyConfigResponse.newBuilder().build();
    }
  }
}
