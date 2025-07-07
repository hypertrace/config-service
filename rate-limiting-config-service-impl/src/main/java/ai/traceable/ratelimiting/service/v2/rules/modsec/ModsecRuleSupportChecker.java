package ai.traceable.ratelimiting.service.v2.rules.modsec;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import jakarta.inject.Singleton;

@Singleton
public class ModsecRuleSupportChecker {
  public static boolean meetsInlineTracingAgentActionRequirements(
      TransactionActionConfig transactionActionConfig) {
    Action action = transactionActionConfig.getAction();
    return action.hasAllow() || action.hasBlock();
  }
}
