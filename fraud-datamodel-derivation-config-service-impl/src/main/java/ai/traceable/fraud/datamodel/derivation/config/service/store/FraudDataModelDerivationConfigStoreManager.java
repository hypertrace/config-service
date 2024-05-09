package ai.traceable.fraud.datamodel.derivation.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfig;
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
    DerivationConfig existingDerivationConfig =
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
