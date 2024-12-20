package ai.traceable.risk.config.service.factors.processor;

import ai.traceable.risk.config.service.RiskConfigConstants;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class RiskFactorConfigStore extends IdentifiedObjectStore<RiskFactorConfig> {

  private final RiskConfigConverter<RiskFactorConfig> configConverter;
  private final RiskConfigUtils<RiskFactorConfig> configUtils;

  @Inject
  protected RiskFactorConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskFactorConfig> configConverter,
      RiskConfigUtils<RiskFactorConfig> configUtils,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        RiskConfigConstants.RISK_CONFIG_NAMESPACE,
        RiskConfigConstants.RISK_FACTOR_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.configConverter = configConverter;
    this.configUtils = configUtils;
  }

  @SneakyThrows
  protected Optional<RiskFactorConfig> buildDataFromValue(Value value) {
    return Optional.of(configConverter.convert(value, configUtils.getNewBuilder()));
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(RiskFactorConfig data) {
    return configConverter.convert(data);
  }

  @Override
  protected String getContextFromData(RiskFactorConfig data) {
    return data.getId();
  }
}
