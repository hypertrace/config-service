package ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils;

import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_ALLOWED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_SNOOZED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_SUSPENDED;

import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.blocking.config.service.v1.BlockingStatus;
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

  public BlockingStatus generateBlockingStatus(
      long expirationMillis, BlockingRuleType blockingRuleType) {
    if (expirationMillis == 0) {
      if (blockingRuleType == BLOCKING_RULE_TYPE_ALLOW) {
        return BLOCKING_STATUS_ALLOWED;
      } else {
        return BLOCKING_STATUS_DENIED;
      }
    } else {
      if (blockingRuleType == BLOCKING_RULE_TYPE_ALLOW) {
        return BLOCKING_STATUS_SNOOZED;
      } else {
        return BLOCKING_STATUS_SUSPENDED;
      }
    }
  }
}
