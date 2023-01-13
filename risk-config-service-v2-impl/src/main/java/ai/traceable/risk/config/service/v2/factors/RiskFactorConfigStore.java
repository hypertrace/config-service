package ai.traceable.risk.config.service.v2.factors;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskConfigConstants;
import ai.traceable.risk.config.service.v2.RiskConfigConverter;
import ai.traceable.risk.config.service.v2.RiskConfigIdGenerator;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class RiskFactorConfigStore extends IdentifiedObjectStore<RiskFactorConfig> {

  private final RiskConfigConverter<RiskFactorConfig> configConverter;
  private final RiskConfigBuilder<RiskFactorConfig> factorConfigBuilder;
  private final RiskConfigIdGenerator configIdGenerator;

  @Inject
  protected RiskFactorConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskFactorConfig> configConverter,
      RiskConfigBuilder<RiskFactorConfig> factorConfigBuilder,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RiskConfigIdGenerator configIdGenerator) {
    super(
        configServiceBlockingStub,
        RiskConfigConstants.RISK_CONFIG_NAMESPACE,
        RiskConfigConstants.RISK_FACTOR_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.configConverter = configConverter;
    this.factorConfigBuilder = factorConfigBuilder;
    this.configIdGenerator = configIdGenerator;
  }

  @Override
  protected Optional<RiskFactorConfig> buildDataFromValue(Value value) {
    try {
      return Optional.of(configConverter.convert(value, factorConfigBuilder.getNewBuilder()));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(String.format("Unable to build data from value: %s", value), e);
    }
  }

  @Override
  protected Value buildValueFromData(RiskFactorConfig data) {
    try {
      return configConverter.convert(data);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(String.format("Unable to build value from data: %s", data), e);
    }
  }

  @Override
  protected String getContextFromData(RiskFactorConfig data) {
    return configIdGenerator.generateId(
        data.getRiskFactorCategory().name(), data.getRiskConfigScope());
  }
}
