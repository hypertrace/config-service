package ai.traceable.risk.config.service.v2.scope;

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
}
