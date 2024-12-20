package ai.traceable.risk.config.service.v2;

import ai.traceable.config.utils.UuidGenerator;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskConfigIdGenerator {

  private final UuidGenerator uuidGenerator;

  public String generateId(String name, RiskConfigScope riskConfigScope) {
    String ID_STRING = name.concat("_");
    if (riskConfigScope.hasEnvironmentScope()) {
      ID_STRING = ID_STRING.concat(riskConfigScope.getEnvironmentScope().getEnvironmentId());
    } else {
      ID_STRING = ID_STRING.concat("GLOBAL");
    }
    return uuidGenerator.generateId(ID_STRING);
  }
}
