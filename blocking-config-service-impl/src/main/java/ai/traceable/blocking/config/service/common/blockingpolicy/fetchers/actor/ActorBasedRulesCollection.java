package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ActorBasedRulesCollection {
  private final List<BlockingPolicyData> threatActorBasedIpViolations;
  private final List<BlockingPolicyData> threatActorBasedIpExemptions;
  private final List<BlockingPolicyData> rateLimitBasedIpViolations;
  private final List<BlockingPolicyData> emailDomainBasedExemptions;
  private final List<BlockingPolicyData> emailDomainBasedViolations;
}
