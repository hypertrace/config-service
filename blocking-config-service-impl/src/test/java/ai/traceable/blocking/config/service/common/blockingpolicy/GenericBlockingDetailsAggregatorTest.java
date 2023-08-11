package ai.traceable.blocking.config.service.common.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator.BlockingDetailsConverterBase;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyAggregate;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class GenericBlockingDetailsAggregatorTest {

  @Test
  void TestGetBlockingDetails() {
    BlockingPolicyDataAggregator mockBlockingPolicyDataAggregator =
        Mockito.mock(BlockingPolicyDataAggregator.class);
    BlockingDetailsConverterBase<String> mockBlockingDetailsConverter =
        (BlockingDetailsConverterBase<String>) Mockito.mock(BlockingDetailsConverterBase.class);
    GenericBlockingDetailsAggregator<String> blockingDetailsAggregator =
        new GenericBlockingDetailsAggregator<>(
            mockBlockingPolicyDataAggregator, mockBlockingDetailsConverter);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    BlockingPolicyDataFilter filter =
        BlockingPolicyDataFilter.builder().environmentId(Optional.of("environment")).build();

    BlockingPolicyData blockingPolicyData1 = BlockingPolicyData.builder().info("one").build();
    BlockingPolicyData blockingPolicyData2 = BlockingPolicyData.builder().info("two").build();
    BlockingPolicyData blockingPolicyData3 = BlockingPolicyData.builder().info("three").build();

    BlockingRulesSupplier blockingRulesSupplier = mock(BlockingRulesSupplier.class);

    doReturn(
            new BlockingPolicyAggregate<>(
                List.of(blockingPolicyData1, blockingPolicyData2, blockingPolicyData3)))
        .when(mockBlockingPolicyDataAggregator)
        .getOrderedBlockingRules(requestContext, filter, blockingRulesSupplier);
    doReturn("1-1").when(mockBlockingDetailsConverter).convert(blockingPolicyData1, filter);
    doReturn("2-1").when(mockBlockingDetailsConverter).convert(blockingPolicyData2, filter);
    doReturn(null).when(mockBlockingDetailsConverter).convert(blockingPolicyData3, filter);

    assertEquals(
        List.of("1-1", "2-1"),
        blockingDetailsAggregator
            .getBlockingDetails(requestContext, filter, blockingRulesSupplier)
            .getBlockingPolicyList());
  }
}
