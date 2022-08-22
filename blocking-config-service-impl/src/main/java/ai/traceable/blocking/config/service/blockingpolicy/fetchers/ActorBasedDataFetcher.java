package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.ActorBasedRulesCache;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.ActorBasedRulesCollection;
import com.google.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ActorBasedDataFetcher {
  private final ActorBasedRulesCache activeActorsCache;

  @Inject
  public ActorBasedDataFetcher(ActorBasedRulesCache activeActorsCache) {
    this.activeActorsCache = activeActorsCache;
  }

  public ActorBasedRulesCollection getActorBasedRules(RequestContext requestContext) {
    return activeActorsCache.getActorBasedRules(requestContext.buildInternalContextualKey());
  }
}
