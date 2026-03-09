package ai.traceable.fraud.datamodel.derivation.config.service.entity;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.CreateEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.DeleteEntityDerivationConfigResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfig;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.EntityDerivationConfigSummary;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigSummariesRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigSummariesResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.GetEntityDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigRequest;
import ai.traceable.fraud.datamodel.entity.derivation.config.service.v1.UpdateEntityDerivationConfigResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EntityDerivationConfigStoreManager {

  private final EntityDerivationConfigStore entityDerivationConfigStore;
  private final UuidGenerator uuidGenerator;
  private final DefaultEntityDerivationProvider defaultEntityDerivationProvider;

  @Inject
  public EntityDerivationConfigStoreManager(
      EntityDerivationConfigStore entityDerivationConfigStore,
      UuidGenerator uuidGenerator,
      DefaultEntityDerivationProvider defaultEntityDerivationProvider) {
    this.entityDerivationConfigStore = entityDerivationConfigStore;
    this.uuidGenerator = uuidGenerator;
    this.defaultEntityDerivationProvider = defaultEntityDerivationProvider;
  }

  public CreateEntityDerivationConfigResponse createEntityDerivationConfig(
      RequestContext requestContext, CreateEntityDerivationConfigRequest request) {
    String configId = uuidGenerator.generateRandomId();
    String columnName = generateColumnName(request.getData().getDisplayName());

    EntityDerivationConfig config =
        EntityDerivationConfig.newBuilder()
            .setId(configId)
            .setColumnName(columnName)
            .setData(request.getData())
            .build();

    ContextualConfigObject<EntityDerivationConfig> configObject =
        entityDerivationConfigStore.upsertObject(requestContext, config);
    EntityDerivationConfig created = buildEntityDerivationConfig(configObject);
    return CreateEntityDerivationConfigResponse.newBuilder()
        .setEntityDerivationConfig(created)
        .build();
  }

  public UpdateEntityDerivationConfigResponse updateEntityDerivationConfig(
      RequestContext requestContext, UpdateEntityDerivationConfigRequest request)
      throws StatusException {
    fetchExistingEntityDerivationConfigOrThrow(request.getId(), requestContext);

    String columnName = generateColumnName(request.getData().getDisplayName());

    EntityDerivationConfig updatedConfig =
        EntityDerivationConfig.newBuilder()
            .setId(request.getId())
            .setColumnName(columnName)
            .setData(request.getData())
            .build();

    ContextualConfigObject<EntityDerivationConfig> configObject =
        entityDerivationConfigStore.upsertObject(requestContext, updatedConfig);
    EntityDerivationConfig updated = buildEntityDerivationConfig(configObject);
    return UpdateEntityDerivationConfigResponse.newBuilder()
        .setEntityDerivationConfig(updated)
        .build();
  }

  public GetEntityDerivationConfigSummariesResponse getEntityDerivationConfigSummaries(
      RequestContext requestContext, GetEntityDerivationConfigSummariesRequest request) {
    GetEntityDerivationConfigsRequest configsRequest =
        GetEntityDerivationConfigsRequest.newBuilder().setFilter(request.getFilter()).build();
    List<EntityDerivationConfig> userConfigs =
        entityDerivationConfigStore.getAllConfigData(requestContext, configsRequest);

    List<EntityDerivationConfig> allConfigs =
        mergeDefaultsWithUserConfigs(userConfigs, configsRequest);

    List<EntityDerivationConfigSummary> summaries =
        allConfigs.stream()
            .map(
                config ->
                    EntityDerivationConfigSummary.newBuilder()
                        .setId(config.getId())
                        .setDisplayName(config.getData().getDisplayName())
                        .setEventKind(config.getData().getEventKind())
                        .setDescription(config.getData().getDescription())
                        .build())
            .collect(Collectors.toList());

    return GetEntityDerivationConfigSummariesResponse.newBuilder()
        .addAllSummaries(summaries)
        .build();
  }

  public GetEntityDerivationConfigsResponse getEntityDerivationConfigs(
      RequestContext requestContext, GetEntityDerivationConfigsRequest request) {
    List<EntityDerivationConfig> userConfigs =
        entityDerivationConfigStore.getAllConfigData(requestContext, request);
    List<EntityDerivationConfig> allConfigs = mergeDefaultsWithUserConfigs(userConfigs, request);
    return GetEntityDerivationConfigsResponse.newBuilder()
        .addAllEntityDerivationConfigs(allConfigs)
        .build();
  }

  public DeleteEntityDerivationConfigResponse deleteEntityDerivationConfig(
      RequestContext requestContext, DeleteEntityDerivationConfigRequest request)
      throws StatusException {
    fetchExistingEntityDerivationConfigOrThrow(
        request.getEntityDerivationConfigId(), requestContext);

    entityDerivationConfigStore.deleteObject(requestContext, request.getEntityDerivationConfigId());
    return DeleteEntityDerivationConfigResponse.getDefaultInstance();
  }

  /**
   * Generates a machine-readable column name from display name. Example: "User Email" →
   * "user_email", "API Key!" → "api_key"
   */
  private static String generateColumnName(String displayName) {
    return displayName
        .trim()
        .toLowerCase()
        .replaceAll("[\\s\\-]+", "_") // Replace spaces and hyphens with underscores
        .replaceAll("[^a-z0-9_]", "") // Keep only alphanumeric and underscores
        .replaceAll("_+", "_") // Replace multiple underscores with single
        .replaceAll("^_+|_+$", ""); // Remove leading/trailing underscores
  }

  private EntityDerivationConfig buildEntityDerivationConfig(
      ContextualConfigObject<EntityDerivationConfig> configObject) {
    return configObject.getData();
  }

  private void fetchExistingEntityDerivationConfigOrThrow(String id, RequestContext requestContext)
      throws StatusException {
    EntityDerivationConfig defaultEntity = defaultEntityDerivationProvider.getDefaultEntity(id);
    if (defaultEntity != null) {
      return;
    }
    entityDerivationConfigStore
        .getData(requestContext, id)
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription("No entity derivation config found with id=" + id)
                    .asException());
  }

  private List<EntityDerivationConfig> mergeDefaultsWithUserConfigs(
      List<EntityDerivationConfig> userConfigs, GetEntityDerivationConfigsRequest request) {
    List<EntityDerivationConfig> defaultConfigs =
        defaultEntityDerivationProvider.getDefaultEntityDerivations();

    // Build set of user config IDs for quick lookup
    Set<String> userConfigIds =
        userConfigs.stream().map(EntityDerivationConfig::getId).collect(Collectors.toSet());

    // Filter defaults: apply filters AND exclude any that have been overridden by user configs
    List<EntityDerivationConfig> filteredDefaults =
        defaultConfigs.stream()
            .filter(config -> EntityDerivationConfigFilterUtil.applyEntityFilter(config, request))
            .filter(config -> !userConfigIds.contains(config.getId()))
            .collect(Collectors.toList());

    // User configs take precedence - add them first, then non-overridden defaults
    List<EntityDerivationConfig> result = new ArrayList<>(userConfigs);
    result.addAll(filteredDefaults);
    return result;
  }
}
