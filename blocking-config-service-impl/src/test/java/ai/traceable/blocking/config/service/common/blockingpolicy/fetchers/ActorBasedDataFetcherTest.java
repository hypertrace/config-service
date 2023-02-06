package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCache;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCollection;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class ActorBasedDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  @Test
  void getActorBasedRulesTest() {
    ActorBasedRulesCollection mockActorBasedRulesCollection = mock(ActorBasedRulesCollection.class);
    ActorBasedRulesCache actorBasedRulesCache = mock(ActorBasedRulesCache.class);

    doReturn(mockActorBasedRulesCollection)
        .when(actorBasedRulesCache)
        .getActorBasedRules(REQUEST_CONTEXT.buildInternalContextualKey(Optional.empty()));

    ActorBasedDataFetcher actorBasedDataFetcher = new ActorBasedDataFetcher(actorBasedRulesCache);
    assertEquals(
        mockActorBasedRulesCollection,
        actorBasedDataFetcher.getActorBasedRules(REQUEST_CONTEXT, Optional.empty()));
  }
}
