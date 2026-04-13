package ai.traceable.ratelimiting.service.v2.rules.modsec;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import jakarta.inject.Singleton;
import java.util.List;

@Singleton
public class ModsecRuleSupportChecker {
  public static boolean meetsInlineTracingAgentActionRequirements(
      TransactionActionConfig transactionActionConfig) {
    Action action = transactionActionConfig.getAction();
    return action.hasAllow() || action.hasBlock();
  }

  public static boolean meetsInlineTracingAgentActionRequirements(
      List<ThresholdActionConfig> thresholdActionConfigs) {
    return thresholdActionConfigs.stream()
        .flatMap(config -> config.getActionsList().stream())
        .anyMatch(action -> action.hasBlock() && !action.getBlock().getUseThresholdDuration());
  }

  public static boolean meetsInlineTracingAgentActionRequirements(
      TransactionActionConfig transactionActionConfig,
      List<ThresholdActionConfig> thresholdActionConfigs) {
    return meetsInlineTracingAgentActionRequirements(transactionActionConfig)
        || meetsInlineTracingAgentActionRequirements(thresholdActionConfigs);
  }
}
