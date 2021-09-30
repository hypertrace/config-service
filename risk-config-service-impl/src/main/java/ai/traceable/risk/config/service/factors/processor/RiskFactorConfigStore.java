package ai.traceable.risk.config.service.factors.processor;

import ai.traceable.risk.config.service.RiskConfigConstants;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class RiskFactorConfigStore extends IdentifiedObjectStore<RiskFactorConfig> {

  private final RiskConfigConverter<RiskFactorConfig> configConverter;
  private final RiskConfigUtils<RiskFactorConfig> configUtils;

  @Inject
  protected RiskFactorConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskFactorConfig> configConverter,
      RiskConfigUtils<RiskFactorConfig> configUtils) {
    super(
        configServiceBlockingStub,
        RiskConfigConstants.RISK_CONFIG_NAMESPACE,
        RiskConfigConstants.RISK_FACTOR_CONFIG_RESOURCE_NAME);
    this.configConverter = configConverter;
    this.configUtils = configUtils;
  }

  @Override
  @SneakyThrows
  protected Optional<RiskFactorConfig> buildObjectFromValue(Value value) {
    return Optional.of(configConverter.convert(value, configUtils.getNewBuilder()));
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromObject(RiskFactorConfig object) {
    return configConverter.convert(object);
  }

  @Override
  protected String getContextFromObject(RiskFactorConfig object) {
    return object.getId();
  }
}
