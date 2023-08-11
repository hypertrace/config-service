package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCache;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import com.google.inject.Inject;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

class ActorBasedBlockingPolicyDataFetcher implements BlockingPolicyDataFetcherBase {
  private final ActorBasedRulesCache activeActorsCache;

  @Inject
  ActorBasedBlockingPolicyDataFetcher(ActorBasedRulesCache activeActorsCache) {
    this.activeActorsCache = activeActorsCache;
  }

  @Override
  public BlockingPolicyAggregate<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext,
      BlockingPolicyDataFilter filter,
      BlockingRulesSupplier blockingRulesSupplier) {
    Optional<String> environmentId = filter.getEnvironmentId();
    return new BlockingPolicyAggregate(
        activeActorsCache.getActorBasedRules(
            requestContext.buildInternalContextualKey(environmentId)));
  }
}
