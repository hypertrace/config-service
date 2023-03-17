package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCache;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class ActorBasedDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  @Test
  void getActorBasedRulesTest() {
    ActorBasedRulesCache actorBasedRulesCache = mock(ActorBasedRulesCache.class);
    List<BlockingPolicyData> blockingPolicyDataList = new ArrayList<>();
    ActorBasedDataFetcher actorBasedDataFetcher = new ActorBasedDataFetcher(actorBasedRulesCache);
    assertEquals(
        blockingPolicyDataList,
        actorBasedDataFetcher.getBlockingPolicyData(REQUEST_CONTEXT, Optional.empty()));
  }
}
