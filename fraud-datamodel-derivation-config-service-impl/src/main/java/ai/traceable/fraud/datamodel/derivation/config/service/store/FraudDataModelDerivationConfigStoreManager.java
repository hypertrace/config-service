package ai.traceable.fraud.datamodel.derivation.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfig;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateDerivationConfigResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.List;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudDataModelDerivationConfigStoreManager {

  private final FraudDataModelDerivationConfigStore fraudDataModelDerivationConfigStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public FraudDataModelDerivationConfigStoreManager(
      FraudDataModelDerivationConfigStore fraudDataModelDerivationConfigStore,
      UuidGenerator uuidGenerator) {
    this.fraudDataModelDerivationConfigStore = fraudDataModelDerivationConfigStore;
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
    // check if we have an existing config with the given id, before proceeding
    fetchExistingDerivationConfigOrThrow(request.getDerivationConfig().getId(), requestContext);
    DerivationConfig updatedDerivationConfig =
        DerivationConfig.newBuilder(request.getDerivationConfig()).build();
    ContextualConfigObject<DerivationConfig> configObject =
        fraudDataModelDerivationConfigStore.upsertObject(requestContext, updatedDerivationConfig);
    DerivationConfig derivationConfig = buildDerivationConfig(configObject);
    return UpdateDerivationConfigResponse.newBuilder()
        .setDerivationConfig(derivationConfig)
        .build();
  }

  public GetDerivationConfigsResponse fetchDerivedConfigs(
      RequestContext requestContext, GetDerivationConfigsRequest request) {
    List<DerivationConfig> derivationConfigs =
        fraudDataModelDerivationConfigStore.getAllConfigData(requestContext, request);
    return GetDerivationConfigsResponse.newBuilder()
        .addAllDerivationConfigs(derivationConfigs)
        .build();
  }

  public GetDerivationConfigResponse fetchDerivedConfig(
      RequestContext requestContext, GetDerivationConfigRequest request) throws StatusException {
    GetDerivationConfigsRequest getDerivationConfigsRequest =
        GetDerivationConfigsRequest.newBuilder()
            .setDerivationConfigType(request.getDerivationConfigType())
            .addDerivationConfigIds(request.getDerivationConfigId())
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
    fraudDataModelDerivationConfigStore.deleteObject(requestContext, id);
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
