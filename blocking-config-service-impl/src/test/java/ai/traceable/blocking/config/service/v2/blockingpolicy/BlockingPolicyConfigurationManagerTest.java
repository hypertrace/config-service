package ai.traceable.blocking.config.service.v2.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfiguration;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BlockingPolicyConfigurationManagerTest {

  @Test
  void getBlockingPolicyConfiguration() {
    GenericBlockingDetailsAggregator<BlockingDetails> mockAggregator =
        (GenericBlockingDetailsAggregator<BlockingDetails>)
            Mockito.mock(GenericBlockingDetailsAggregator.class);

    BlockingPolicyConfigurationManager manager =
        new DefaultBlockingPolicyConfigurationManager(mockAggregator);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    Optional<String> environmentId = Optional.of("environment");

    BlockingDetails blockingDetails1 = Mockito.mock(BlockingDetails.class);
    BlockingDetails blockingDetails2 = Mockito.mock(BlockingDetails.class);
    List<BlockingDetails> mockBlockingDetailsList = List.of(blockingDetails1, blockingDetails2);

    doReturn(mockBlockingDetailsList)
        .when(mockAggregator)
        .getBlockingDetails(requestContext, environmentId);

    assertEquals(
        BlockingPolicyConfiguration.newBuilder()
            .addAllBlockingDetailsList(mockBlockingDetailsList)
            .build(),
        manager.getBlockingPolicyConfiguration(requestContext, environmentId));
  }
}
