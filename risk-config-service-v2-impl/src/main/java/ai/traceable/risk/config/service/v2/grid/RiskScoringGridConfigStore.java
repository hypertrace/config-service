package ai.traceable.risk.config.service.v2.grid;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskConfigConstants;
import ai.traceable.risk.config.service.v2.RiskConfigConverter;
import ai.traceable.risk.config.service.v2.RiskConfigIdGenerator;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class RiskScoringGridConfigStore extends IdentifiedObjectStore<RiskScoringGridConfigValues> {

  private final RiskConfigConverter<RiskScoringGridConfigValues> configConverter;
  private final RiskConfigBuilder<RiskScoringGridConfigValues> riskConfigBuilder;
  private final RiskConfigIdGenerator configIdGenerator;
  private final String ID_NAME = "RiskScoringGrid";

  @Inject
  protected RiskScoringGridConfigStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      RiskConfigConverter<RiskScoringGridConfigValues> configConverter,
      RiskConfigBuilder<RiskScoringGridConfigValues> riskConfigBuilder,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RiskConfigIdGenerator configIdGenerator) {
    super(
        configServiceBlockingStub,
        RiskConfigConstants.RISK_CONFIG_NAMESPACE,
        RiskConfigConstants.RISK_SCORING_GRID_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.configConverter = configConverter;
    this.riskConfigBuilder = riskConfigBuilder;
    this.configIdGenerator = configIdGenerator;
  }

  @Override
  protected Optional<RiskScoringGridConfigValues> buildDataFromValue(Value value) {
    try {
      return Optional.of(configConverter.convert(value, riskConfigBuilder.getNewBuilder()));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(String.format("Unable to build data from value: %s", value), e);
    }
  }

  @Override
  protected Value buildValueFromData(RiskScoringGridConfigValues data) {
    try {
      return configConverter.convert(data);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(String.format("Unable to build value from data: %s", data), e);
    }
  }

  @Override
  protected String getContextFromData(RiskScoringGridConfigValues data) {
    return configIdGenerator.generateId(ID_NAME, data.getRiskConfigScope());
  }
}
