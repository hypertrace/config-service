package ai.traceable.blocking.config.service.common.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator.BlockingDetailsConverterBase;
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
    Optional<String> environmentId = Optional.of("environment");

    BlockingPolicyData blockingPolicyData1 = BlockingPolicyData.builder().info("one").build();
    BlockingPolicyData blockingPolicyData2 = BlockingPolicyData.builder().info("two").build();

    doReturn(List.of(blockingPolicyData1, blockingPolicyData2))
        .when(mockBlockingPolicyDataAggregator)
        .getOrderedBlockingRules(requestContext, environmentId);
    doReturn(List.of("1-1", "1-2")).when(mockBlockingDetailsConverter).convert(blockingPolicyData1);
    doReturn(List.of("2-1", "2-2")).when(mockBlockingDetailsConverter).convert(blockingPolicyData2);

    assertEquals(
        List.of("1-1", "1-2", "2-1", "2-2"),
        blockingDetailsAggregator.getBlockingDetails(requestContext, environmentId));
  }
}
