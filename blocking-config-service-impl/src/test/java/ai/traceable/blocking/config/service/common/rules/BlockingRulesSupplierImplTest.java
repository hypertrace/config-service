package ai.traceable.blocking.config.service.common.rules;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import ai.traceable.blocking.config.service.common.rules.fetchers.IpResolutionStrategyFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher.RulesFetcherType;
import ai.traceable.blocking.config.service.v2.IpResolutionStrategy;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BlockingRulesSupplierImplTest {

  @Test
  @DisplayName("getIpResolutionStrategies delegates to fetcher and returns result")
  void getIpResolutionStrategies_success() {
    BlockingRulesSupplierContext ctx = mock(BlockingRulesSupplierContext.class);
    IpResolutionStrategyFetcher fetcher = mock(IpResolutionStrategyFetcher.class);
    when(ctx.getRulesFetcher(RulesFetcherType.IP_RESOLUTION_STRATEGY)).thenReturn(fetcher);

    IpResolutionStrategy strategy =
        IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true).build();
    when(fetcher.fetchStrategies(any(), any(), any())).thenReturn(Map.of("svc1", strategy));

    BlockingRulesSupplierImpl supplier =
        new BlockingRulesSupplierImpl(ctx, RequestContext.forTenantId("t"), Optional.of("prod"));

    Map<String, IpResolutionStrategy> result = supplier.getIpResolutionStrategies(Set.of("svc1"));
    assertEquals(1, result.size());
    assertEquals(strategy, result.get("svc1"));
  }

  @Test
  @DisplayName("getIpResolutionStrategies returns empty on empty input and does not call fetcher")
  void getIpResolutionStrategies_emptyInput() {
    BlockingRulesSupplierContext ctx = mock(BlockingRulesSupplierContext.class);
    IpResolutionStrategyFetcher fetcher = mock(IpResolutionStrategyFetcher.class);
    when(ctx.getRulesFetcher(RulesFetcherType.IP_RESOLUTION_STRATEGY)).thenReturn(fetcher);

    BlockingRulesSupplierImpl supplier =
        new BlockingRulesSupplierImpl(ctx, RequestContext.forTenantId("t"), Optional.empty());

    Map<String, IpResolutionStrategy> result = supplier.getIpResolutionStrategies(Set.of());
    assertTrue(result.isEmpty());
    verify(fetcher, never()).fetchStrategies(any(), any(), any());
  }

  @Test
  @DisplayName("getIpResolutionStrategies swallows exceptions and returns empty")
  void getIpResolutionStrategies_exception() {
    BlockingRulesSupplierContext ctx = mock(BlockingRulesSupplierContext.class);
    IpResolutionStrategyFetcher fetcher = mock(IpResolutionStrategyFetcher.class);
    when(ctx.getRulesFetcher(RulesFetcherType.IP_RESOLUTION_STRATEGY)).thenReturn(fetcher);
    when(fetcher.fetchStrategies(any(), any(), any())).thenThrow(new RuntimeException("boom"));

    BlockingRulesSupplierImpl supplier =
        new BlockingRulesSupplierImpl(ctx, RequestContext.forTenantId("t"), Optional.of("prod"));

    Map<String, IpResolutionStrategy> result = supplier.getIpResolutionStrategies(Set.of("svc1"));
    assertTrue(result.isEmpty());
  }
}
