package ai.traceable.risk.config.service.factors.processor;

import ai.traceable.risk.config.service.RiskConfigConstants;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class RiskElementConfigStore extends IdentifiedObjectStore<RiskElementConfig> {

  private final RiskConfigConverter<RiskElementConfig> configConverter;
  private final RiskConfigUtils<RiskElementConfig> configUtils;

  @Inject
  protected RiskElementConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskElementConfig> configConverter,
      RiskConfigUtils<RiskElementConfig> configUtils) {
    super(
        configServiceBlockingStub,
        RiskConfigConstants.RISK_CONFIG_NAMESPACE,
        RiskConfigConstants.RISK_ELEMENT_CONFIG_RESOURCE_NAME);
    this.configConverter = configConverter;
    this.configUtils = configUtils;
  }

  @Override
  @SneakyThrows
  protected Optional<RiskElementConfig> buildObjectFromValue(Value value) {
    return Optional.of(configConverter.convert(value, configUtils.getNewBuilder()));
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromObject(RiskElementConfig object) {
    return configConverter.convert(object);
  }

  @Override
  protected String getContextFromObject(RiskElementConfig object) {
    return object.getId();
  }
}
