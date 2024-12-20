package ai.traceable.risk.config.service.factors.processor;

import ai.traceable.risk.config.service.RiskConfigConstants;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class RiskElementConfigStore extends IdentifiedObjectStore<RiskElementConfig> {

  private final RiskConfigConverter<RiskElementConfig> configConverter;
  private final RiskConfigUtils<RiskElementConfig> configUtils;

  @Inject
  protected RiskElementConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskElementConfig> configConverter,
      RiskConfigUtils<RiskElementConfig> configUtils,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        RiskConfigConstants.RISK_CONFIG_NAMESPACE,
        RiskConfigConstants.RISK_ELEMENT_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.configConverter = configConverter;
    this.configUtils = configUtils;
  }

  @Override
  @SneakyThrows
  protected Optional<RiskElementConfig> buildDataFromValue(Value value) {
    return Optional.of(configConverter.convert(value, configUtils.getNewBuilder()));
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(RiskElementConfig data) {
    return configConverter.convert(data);
  }

  @Override
  protected String getContextFromData(RiskElementConfig data) {
    return data.getId();
  }
}
