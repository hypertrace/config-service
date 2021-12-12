package ai.traceable.risk.config.service.level.processor;

import ai.traceable.risk.config.service.RiskConfigConstants;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class RiskLevelConfigStore extends DefaultObjectStore<RiskLevelConfigValues> {

  private final RiskConfigConverter<RiskLevelConfigValues> configConverter;
  private final RiskConfigUtils<RiskLevelConfigValues> configUtils;

  @Inject
  protected RiskLevelConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskLevelConfigValues> configConverter,
      RiskConfigUtils<RiskLevelConfigValues> configUtils,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        RiskConfigConstants.RISK_CONFIG_NAMESPACE,
        RiskConfigConstants.RISK_LEVEL_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.configConverter = configConverter;
    this.configUtils = configUtils;
  }

  @Override
  @SneakyThrows
  protected Optional<RiskLevelConfigValues> buildDataFromValue(Value value) {
    return Optional.of(configConverter.convert(value, configUtils.getNewBuilder()));
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(RiskLevelConfigValues data) {
    return configConverter.convert(data);
  }
}
