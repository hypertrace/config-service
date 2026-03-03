package ai.traceable.risk.config.service.v2;

import ai.traceable.config.utils.UuidGenerator;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskConfigIdGenerator {

  private static final String GLOBAL_ENVIRONMENT_ID = "GLOBAL";
  private final UuidGenerator uuidGenerator;

  public String generateId(String name, RiskConfigScope riskConfigScope) {
    StringBuilder idString = new StringBuilder(name).append("_");
    EntityType entityType = riskConfigScope.getEntityType();
    if (entityType != EntityType.ENTITY_TYPE_UNSPECIFIED
        && entityType != EntityType.ENTITY_TYPE_API) {
      // We skip adding the entity type for APIs, for backward compatibility
      idString.append(entityType.name()).append("_");
    }
    if (riskConfigScope.hasEnvironmentScope()) {
      idString.append(riskConfigScope.getEnvironmentScope().getEnvironmentId());
    } else {
      idString.append(GLOBAL_ENVIRONMENT_ID);
    }
    return uuidGenerator.generateId(idString.toString());
  }
}
