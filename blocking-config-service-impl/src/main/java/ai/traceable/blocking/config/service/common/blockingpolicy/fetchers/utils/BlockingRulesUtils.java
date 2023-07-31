package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Status;
import com.google.inject.Inject;
import java.time.Clock;

public class BlockingRulesUtils {
  private final Clock clock;

  @Inject
  public BlockingRulesUtils(Clock clock) {
    this.clock = clock;
  }

  public boolean isRuleActive(long expirationMillis) {
    return expirationMillis == 0 || expirationMillis > clock.instant().toEpochMilli();
  }

  public Status generateBlockingStatus(long expirationMillis, RuleType ruleType) {
    if (expirationMillis == 0) {
      return (ruleType == RuleType.ALLOW) ? Status.ALLOWED : Status.DENIED;
    } else {
      return (ruleType == RuleType.ALLOW) ? Status.SNOOZED : Status.SUSPENDED;
    }
  }
}
