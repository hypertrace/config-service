package ai.traceable.risk.config.service.v2.scope;

import ai.traceable.risk.config.service.v2.EntityType;
import ai.traceable.risk.config.service.v2.EnvironmentScope;
import ai.traceable.risk.config.service.v2.RiskConfigScope;

public class MockScopeData {

  public static RiskConfigScope getGlobalRiskConfigScope() {
    return RiskConfigScope.getDefaultInstance();
  }

  public static RiskConfigScope getEnvironmentBasedScope() {
    return RiskConfigScope.newBuilder()
        .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("environment"))
        .build();
  }

  public static RiskConfigScope getMcpToolEntityTypeScope() {
    return RiskConfigScope.newBuilder().setEntityType(EntityType.ENTITY_TYPE_MCP_TOOL).build();
  }

  public static RiskConfigScope getApiEntityTypeAndEnvironmentScope() {
    return RiskConfigScope.newBuilder()
        .setEntityType(EntityType.ENTITY_TYPE_API)
        .setEnvironmentScope(EnvironmentScope.newBuilder().setEnvironmentId("environment"))
        .build();
  }
}
