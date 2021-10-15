package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG;
import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.SENSITIVE_DATA_CONFIGURATION;

import ai.traceable.sensitivedata.config.service.v1.ParamType;
import com.google.protobuf.Value;
import java.util.Optional;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

class ParamTypeRedactionStrategyConfigStore
    extends IdentifiedObjectStore<ParamTypeRedactionStrategyConfig> {

  private final ParamType paramType;

  private ParamTypeRedactionStrategyConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ParamType paramType) {
    super(
        configServiceBlockingStub,
        SENSITIVE_DATA_CONFIGURATION,
        PARAMETER_TYPE_REDACTION_STRATEGY_CONFIG,
        configChangeEventGenerator);
    this.paramType = paramType;
  }

  static ParamTypeRedactionStrategyConfigStore createInstance(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ParamType paramType) {
    return new ParamTypeRedactionStrategyConfigStore(
        configServiceBlockingStub, configChangeEventGenerator, paramType);
  }

  @Override
  protected Optional<ParamTypeRedactionStrategyConfig> buildObjectFromValue(Value value) {
    return ParamTypeRedactionStrategyConfig.fromValue(value);
  }

  @Override
  protected Value buildValueFromObject(
      ParamTypeRedactionStrategyConfig paramTypeRedactionStrategyConfig) {
    return paramTypeRedactionStrategyConfig.toValue();
  }

  @Override
  protected String getContextFromObject(ParamTypeRedactionStrategyConfig object) {
    return paramType.name();
  }
}
