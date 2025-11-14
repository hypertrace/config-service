package ai.traceable.fraud.datamodel.derivation.config.service.store;

import static ai.traceable.config.proto.utils.FieldMaskUtils.applyFieldMask;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.derivation.config.service.DefaultFraudDataModelDerivationConfig;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.*;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudDataModelDerivationConfigStoreManager {

  private final FraudDataModelDerivationConfigStore fraudDataModelDerivationConfigStore;
  private final FraudDataModelDerivationTransformConfigStore
      fraudDataModelDerivationTransformConfigStore;
  private final DefaultFraudDataModelDerivationConfig defaultFraudDataModelDerivationConfig;
  private final UuidGenerator uuidGenerator;

  @Inject
  public FraudDataModelDerivationConfigStoreManager(
      FraudDataModelDerivationConfigStore fraudDataModelDerivationConfigStore,
      FraudDataModelDerivationTransformConfigStore fraudDataModelDerivationTransformConfigStore,
      DefaultFraudDataModelDerivationConfig defaultFraudDataModelDerivationConfig,
      UuidGenerator uuidGenerator) {
    this.fraudDataModelDerivationConfigStore = fraudDataModelDerivationConfigStore;
    this.fraudDataModelDerivationTransformConfigStore =
        fraudDataModelDerivationTransformConfigStore;
    this.defaultFraudDataModelDerivationConfig = defaultFraudDataModelDerivationConfig;
    this.uuidGenerator = uuidGenerator;
  }

  public CreateDerivationConfigResponse createDerivationConfig(
      RequestContext requestContext, CreateDerivationConfigRequest request) {
    DerivationConfig newSavedFilter =
        DerivationConfig.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setName(request.getName())
            .setDerivationConfigType(request.getDerivationConfigType())
            .setDerivationConfig(request.getDerivationConfig())
            .setDisabled(request.getDisabled())
            .setDescription(request.getDescription())
            .build();
    ContextualConfigObject<DerivationConfig> configObject =
        fraudDataModelDerivationConfigStore.upsertObject(requestContext, newSavedFilter);
    DerivationConfig derivationConfig = buildDerivationConfig(configObject);
    return CreateDerivationConfigResponse.newBuilder()
        .setDerivationConfig(derivationConfig)
        .build();
  }

  public UpdateDerivationConfigResponse updateDerivationConfig(
      RequestContext requestContext, UpdateDerivationConfigRequest request) throws StatusException {
    if (defaultFraudDataModelDerivationConfig.isDefaultConfig(
        request.getDerivationConfig().getId())) {
      throw Status.UNIMPLEMENTED
          .withDescription("Edit operation is not supported for default derivation configs")
          .asRuntimeException(requestContext.buildTrailers());
    }

    // check if we have an existing config with the given id, before proceeding
    var existing =
        fetchExistingDerivationConfigOrThrow(request.getDerivationConfig().getId(), requestContext);
    DerivationConfig updatedResource =
        applyFieldMask(existing, request.getDerivationConfig(), request.getUpdateMask());

    ContextualConfigObject<DerivationConfig> configObject =
        fraudDataModelDerivationConfigStore.upsertObject(requestContext, updatedResource);
    DerivationConfig derivationConfig = buildDerivationConfig(configObject);
    return UpdateDerivationConfigResponse.newBuilder()
        .setDerivationConfig(derivationConfig)
        .build();
  }

  public UpsertDerivationConfigResponse upsertDerivationConfig(
      RequestContext requestContext, UpsertDerivationConfigRequest request) throws StatusException {
    if (defaultFraudDataModelDerivationConfig.isDefaultConfig(
        request.getDerivationConfig().getId())) {
      throw Status.UNIMPLEMENTED
          .withDescription("Edit operation is not supported for default derivation configs")
          .asRuntimeException(requestContext.buildTrailers());
    }

    DerivationConfig updatedDerivationConfig =
        DerivationConfig.newBuilder(request.getDerivationConfig()).build();
    ContextualConfigObject<DerivationConfig> configObject =
        fraudDataModelDerivationConfigStore.upsertObject(requestContext, updatedDerivationConfig);
    DerivationConfig derivationConfig = buildDerivationConfig(configObject);
    return UpsertDerivationConfigResponse.newBuilder()
        .setDerivationConfig(derivationConfig)
        .build();
  }

  public GetDerivationConfigsResponse fetchDerivedConfigs(
      RequestContext requestContext, GetDerivationConfigsRequest request) {
    List<DerivationConfig> storedDerivationConfigs =
        fraudDataModelDerivationConfigStore.getAllConfigData(requestContext, request);

    return GetDerivationConfigsResponse.newBuilder()
        .addAllDerivationConfigs(
            defaultFraudDataModelDerivationConfig.getDerivationConfigsForType(
                request.getDerivationConfigType()))
        .addAllDerivationConfigs(storedDerivationConfigs)
        .build();
  }

  public GetDerivationConfigResponse fetchDerivedConfig(
      RequestContext requestContext, GetDerivationConfigRequest request) throws StatusException {
    Optional<DerivationConfig> defaultConfig =
        defaultFraudDataModelDerivationConfig.getDefaultConfig(request.getDerivationConfigId());
    if (defaultConfig.isPresent()) {
      return GetDerivationConfigResponse.newBuilder()
          .setDerivationConfig(defaultConfig.get())
          .build();
    }

    GetDerivationConfigsRequest getDerivationConfigsRequest =
        GetDerivationConfigsRequest.newBuilder()
            .setDerivationConfigType(request.getDerivationConfigType())
            .addDerivationConfigIds(request.getDerivationConfigId())
            .setIncludeDisabled(request.getIncludeDisabled())
            .build();
    List<DerivationConfig> derivationConfigs =
        fraudDataModelDerivationConfigStore.getAllConfigData(
            requestContext, getDerivationConfigsRequest);
    if (derivationConfigs.isEmpty()) {
      throw Status.NOT_FOUND
          .withDescription("No derivation config of id=" + request.getDerivationConfigId())
          .asException();
    }
    if (derivationConfigs.size() > 1) {
      throw Status.INTERNAL
          .withDescription(
              String.format(
                  "%d derivation configs found with id=%s",
                  derivationConfigs.size(), request.getDerivationConfigId()))
          .asException();
    }
    return GetDerivationConfigResponse.newBuilder()
        .setDerivationConfig(derivationConfigs.get(0))
        .build();
  }

  public void deleteDerivedConfig(RequestContext requestContext, String id) {
    if (defaultFraudDataModelDerivationConfig.isDefaultConfig(id)) {
      throw Status.UNIMPLEMENTED
          .withDescription("Delete operation is not supported for default derivation configs")
          .asRuntimeException(requestContext.buildTrailers());
    }
    fraudDataModelDerivationConfigStore.deleteObject(requestContext, id);
  }

  public CreateUserAgentMergeMappingConfigResponse createUserAgentMergeMappingConfig(
      RequestContext requestContext, CreateUserAgentMergeMappingConfigRequest request) {
    UserAgentMergeMappingConfig.Builder configBuilder = UserAgentMergeMappingConfig.newBuilder();

    configBuilder.setId(uuidGenerator.generateRandomId());
    configBuilder.putAllPairs(request.getPairsMap());
    if (request.hasApiScope()) {
      configBuilder.setApiScope(
          ApiScope.newBuilder().addAllApiIds(request.getApiScope().getApiIdsList()));
    } else {
      configBuilder.setTenantScope(TenantScope.newBuilder().build());
    }

    UserAgentMergeMappingConfig config = configBuilder.build();
    ContextualConfigObject<UserAgentMergeMappingConfig> configObject =
        fraudDataModelDerivationTransformConfigStore.upsertObject(requestContext, config);
    return CreateUserAgentMergeMappingConfigResponse.newBuilder()
        .setUserAgentMergeMappingConfig(configObject.getData())
        .build();
  }

  public GetUserAgentMergeMappingConfigResponse getUserAgentMergeMappingConfig(
      RequestContext requestContext, GetUserAgentMergeMappingConfigRequest request)
      throws StatusException {
    Optional<UserAgentMergeMappingConfig> config =
        fraudDataModelDerivationTransformConfigStore.getData(requestContext, request.getId());
    if (config.isEmpty()) {
      throw Status.INTERNAL
          .withDescription(
              String.format("No user agent merge mapping config found with id=%s", request.getId()))
          .asException();
    }
    return GetUserAgentMergeMappingConfigResponse.newBuilder()
        .setUserAgentMergeMappingConfig(config.get())
        .build();
  }

  public GetUserAgentMergeMappingConfigsResponse getUserAgentMergeMappingConfigs(
      RequestContext requestContext, GetUserAgentMergeMappingConfigsRequest request) {
    List<UserAgentMergeMappingConfig> configs =
        fraudDataModelDerivationTransformConfigStore.getAllConfigData(requestContext);
    return GetUserAgentMergeMappingConfigsResponse.newBuilder()
        .addAllUserAgentMergeMappingConfig(configs)
        .build();
  }

  public UpdateUserAgentMergeMappingConfigResponse updateUserAgentMergeMappingConfig(
      RequestContext requestContext, UpdateUserAgentMergeMappingConfigRequest request) {
    UserAgentMergeMappingConfig.Builder configBuilder = UserAgentMergeMappingConfig.newBuilder();
    configBuilder.setId(request.getUserAgentMergeMappingConfig().getId());
    configBuilder.putAllPairs(request.getUserAgentMergeMappingConfig().getPairsMap());
    if (request.getUserAgentMergeMappingConfig().hasApiScope()) {
      configBuilder.setApiScope(
          ApiScope.newBuilder()
              .addAllApiIds(request.getUserAgentMergeMappingConfig().getApiScope().getApiIdsList())
              .build());
    } else {
      configBuilder.setTenantScope(TenantScope.newBuilder().build());
    }
    UserAgentMergeMappingConfig config = configBuilder.build();

    ContextualConfigObject<UserAgentMergeMappingConfig> configObject =
        fraudDataModelDerivationTransformConfigStore.upsertObject(requestContext, config);

    return UpdateUserAgentMergeMappingConfigResponse.newBuilder()
        .setUserAgentMergeMappingConfig(configObject.getData())
        .build();
  }

  public void deleteUserAgentMergeMappingConfig(
      RequestContext requestContext, DeleteUserAgentMergeMappingConfigRequest request) {
    fraudDataModelDerivationTransformConfigStore.deleteObject(requestContext, request.getId());
  }

  private DerivationConfig buildDerivationConfig(
      ContextualConfigObject<DerivationConfig> configObject) {
    return DerivationConfig.newBuilder(configObject.getData()).build();
  }

  private DerivationConfig fetchExistingDerivationConfigOrThrow(
      String id, RequestContext requestContext) throws StatusException {
    return fraudDataModelDerivationConfigStore
        .getData(requestContext, id)
        .orElseThrow(Status.NOT_FOUND::asException);
  }
}
