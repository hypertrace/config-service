package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCache;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCollection;
import com.google.inject.Inject;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ActorBasedDataFetcher {
  private final ActorBasedRulesCache activeActorsCache;

  @Inject
  public ActorBasedDataFetcher(ActorBasedRulesCache activeActorsCache) {
    this.activeActorsCache = activeActorsCache;
  }

  public ActorBasedRulesCollection getActorBasedRules(
      RequestContext requestContext, Optional<String> environmentId) {
    return activeActorsCache.getActorBasedRules(
        requestContext.buildInternalContextualKey(environmentId));
  }
}
