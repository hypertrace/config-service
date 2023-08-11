package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCache;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class ActorBasedBlockingPolicyDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  @Test
  void getActorBasedRulesTest() {
    ActorBasedRulesCache actorBasedRulesCache = mock(ActorBasedRulesCache.class);
    List<BlockingPolicyData> blockingPolicyDataList = new ArrayList<>();
    ActorBasedBlockingPolicyDataFetcher actorBasedDataFetcher =
        new ActorBasedBlockingPolicyDataFetcher(actorBasedRulesCache);
    assertEquals(
        blockingPolicyDataList,
        actorBasedDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList());
  }
}
