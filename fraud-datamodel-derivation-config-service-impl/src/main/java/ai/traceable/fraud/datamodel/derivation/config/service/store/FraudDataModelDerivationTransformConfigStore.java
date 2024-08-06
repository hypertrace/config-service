package ai.traceable.fraud.datamodel.derivation.config.service.store;

import static ai.traceable.fraud.datamodel.derivation.config.service.store.FraudDataModelDerivationConfigStore.FRAUD_DATAMODEL_DERIVATION_CONFIG_RESOURCE_NAMESPACE;
import static ai.traceable.fraud.datamodel.derivation.config.service.store.FraudDataModelDerivationConfigStore.FRAUD_DATAMODEL_DERIVATION_RESOURCE_NAME;

import ai.traceable.fraud.datamodel.derivation.config.service.v1.UserAgentMergeMappingConfig;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class FraudDataModelDerivationTransformConfigStore
    extends IdentifiedObjectStore<UserAgentMergeMappingConfig> {

  @Inject
  public FraudDataModelDerivationTransformConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        FRAUD_DATAMODEL_DERIVATION_CONFIG_RESOURCE_NAMESPACE,
        FRAUD_DATAMODEL_DERIVATION_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<UserAgentMergeMappingConfig> buildDataFromValue(Value value) {
    UserAgentMergeMappingConfig.Builder builder = UserAgentMergeMappingConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(UserAgentMergeMappingConfig data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(UserAgentMergeMappingConfig data) {
    return data.getId();
  }
}
