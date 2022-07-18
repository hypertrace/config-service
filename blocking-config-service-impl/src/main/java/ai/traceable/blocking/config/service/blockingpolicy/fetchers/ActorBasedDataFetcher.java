package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.v1.BlockingDetails;
import java.util.List;

public interface ActorBasedDataFetcher {
  List<BlockingDetails> getThreatActorBasedIpViolation();

  List<BlockingDetails> getThreatActorBasedIpExemption();

  List<BlockingDetails> getRateLimitBasedIpViolation();
}
