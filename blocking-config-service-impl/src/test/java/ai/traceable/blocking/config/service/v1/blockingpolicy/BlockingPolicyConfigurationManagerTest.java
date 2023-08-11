package ai.traceable.blocking.config.service.v1.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyAggregate;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import ai.traceable.config.utils.UuidGenerator;
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
    UuidGenerator uuidGenerator = Mockito.mock(UuidGenerator.class);

    BlockingPolicyConfigurationManager manager =
        new DefaultBlockingPolicyConfigurationManager(mockAggregator, uuidGenerator);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    Optional<String> environmentId = Optional.of("environment");

    BlockingDetails blockingDetails1 = Mockito.mock(BlockingDetails.class);
    BlockingDetails blockingDetails2 = Mockito.mock(BlockingDetails.class);
    List<BlockingDetails> mockBlockingDetailsList = List.of(blockingDetails1, blockingDetails2);

    BlockingRulesSupplier mockBlockingRulesSupplier = mock(BlockingRulesSupplier.class);
    doReturn(new BlockingPolicyAggregate<>(mockBlockingDetailsList))
        .when(mockAggregator)
        .getBlockingDetails(
            requestContext,
            BlockingPolicyDataFilter.builder().environmentId(environmentId).build(),
            mockBlockingRulesSupplier);
    doReturn("hash").when(uuidGenerator).generateId(mockBlockingDetailsList);

    assertEquals(
        BlockingPolicyConfiguration.newBuilder()
            .addAllBlockingDetailsList(mockBlockingDetailsList)
            .setHash("hash")
            .build(),
        manager.getBlockingPolicyConfiguration(
            requestContext, "", environmentId, mockBlockingRulesSupplier));

    assertEquals(
        BlockingPolicyConfiguration.newBuilder().setHash("hash").build(),
        manager.getBlockingPolicyConfiguration(
            requestContext, "hash", environmentId, mockBlockingRulesSupplier));
  }
}
