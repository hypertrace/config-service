package ai.traceable.risk.config.service.factorgrid.processor;

import ai.traceable.risk.config.service.RiskConfigConstants;
import ai.traceable.risk.config.service.processor.RiskConfigConverter;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.DefaultObjectStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

public class RiskFactorGridConfigStore extends DefaultObjectStore<RiskFactorGridConfigValues> {

  private final RiskConfigConverter<RiskFactorGridConfigValues> configConverter;
  private final RiskConfigUtils<RiskFactorGridConfigValues> configUtils;

  @Inject
  protected RiskFactorGridConfigStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskFactorGridConfigValues> configConverter,
      RiskConfigUtils<RiskFactorGridConfigValues> configUtils) {
    super(
        configServiceBlockingStub,
        RiskConfigConstants.RISK_CONFIG_NAMESPACE,
        RiskConfigConstants.RISK_FACTOR_GRID_CONFIG_RESOURCE_NAME);
    this.configConverter = configConverter;
    this.configUtils = configUtils;
  }

  @Override
  @SneakyThrows
  protected Optional<RiskFactorGridConfigValues> buildObjectFromValue(Value value) {
    return Optional.of(configConverter.convert(value, configUtils.getNewBuilder()));
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromObject(RiskFactorGridConfigValues object) {
    return configConverter.convert(object);
  }
}
