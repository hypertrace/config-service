package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCache;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

class ActorBasedDataFetcher implements DataFetcherBase {
  private final ActorBasedRulesCache activeActorsCache;

  @Inject
  ActorBasedDataFetcher(ActorBasedRulesCache activeActorsCache) {
    this.activeActorsCache = activeActorsCache;
  }

  @Override
  public List<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext, Optional<String> environmentId) {
    return activeActorsCache.getActorBasedRules(
        requestContext.buildInternalContextualKey(environmentId));
  }
}
