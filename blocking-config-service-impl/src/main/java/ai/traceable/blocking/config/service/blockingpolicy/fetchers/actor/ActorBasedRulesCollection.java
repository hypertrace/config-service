package ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor;

import ai.traceable.blocking.config.service.v1.BlockingDetails;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ActorBasedRulesCollection {
  private final List<BlockingDetails> threatActorBasedIpViolations;
  private final List<BlockingDetails> threatActorBasedIpExemptions;
  private final List<BlockingDetails> rateLimitBasedIpViolations;
}
