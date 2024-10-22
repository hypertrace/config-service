package ai.traceable.fraud.datamodel.derivation.config.service.store;

import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfig;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsRequest;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudDataModelDerivationConfigStore
    extends IdentifiedObjectStoreWithFilter<DerivationConfig, GetDerivationConfigsRequest> {

  public static final String FRAUD_DATAMODEL_DERIVATION_RESOURCE_NAME =
      "fraud-datamodel-derivation";
  public static final String FRAUD_DATAMODEL_DERIVATION_CONFIG_RESOURCE_NAMESPACE =
      "fraud-datamodel-derivation-config";

  @Inject
  public FraudDataModelDerivationConfigStore(
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
  protected Optional<DerivationConfig> buildDataFromValue(Value value) {
    DerivationConfig.Builder derivationConfigBuilder = DerivationConfig.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, derivationConfigBuilder);
    return Optional.of(derivationConfigBuilder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(DerivationConfig derivationConfig) {
    return ConfigProtoConverter.convertToValue(derivationConfig);
  }

  @Override
  protected String getContextFromData(DerivationConfig derivationConfig) {
    return derivationConfig.getId();
  }

  @Override
  protected Optional<DerivationConfig> filterConfigData(
      DerivationConfig data, GetDerivationConfigsRequest request) {
    return Optional.of(data)
        .filter(derivationConfig -> request.getIncludeDisabled() || !derivationConfig.getDisabled())
        .filter(
            derivationConfig ->
                request.getDerivationConfigType().equals(data.getDerivationConfigType()))
        .filter(
            derivationConfig ->
                request.getDerivationConfigIdsCount() == 0
                    || request.getDerivationConfigIdsList().contains(data.getId()));
  }

  @Override
  public List<DerivationConfig> getAllConfigData(
      RequestContext requestContext, GetDerivationConfigsRequest request) {
    List<DerivationConfig> derivationConfigs = super.getAllConfigData(requestContext, request);
    return derivationConfigs.stream()
        .filter(derivationConfig -> filterConfigData(derivationConfig, request).isPresent())
        .collect((Collectors.toUnmodifiableList()));
  }
}
