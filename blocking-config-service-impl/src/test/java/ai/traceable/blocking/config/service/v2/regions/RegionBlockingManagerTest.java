package ai.traceable.blocking.config.service.v2.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.regions.GenericRegionRuleAggregator;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.RegionBlockingRules;
import ai.traceable.blocking.config.service.v2.RegionBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.RegionIpBlockingRule;
import ai.traceable.config.utils.UuidGenerator;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class RegionBlockingManagerTest {
  @Test
  void getEnabledBlockingRules() {
    GenericRegionRuleAggregator<RegionIpBlockingRule> mockAggregator =
        (GenericRegionRuleAggregator<RegionIpBlockingRule>) mock(GenericRegionRuleAggregator.class);
    UuidGenerator mockUuidGenerator = mock(UuidGenerator.class);

    BlockingConfigManagerBase manager =
        new RegionBlockingManager(mockAggregator, mockUuidGenerator);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    Optional<String> environmentId = Optional.of("environment");

    RegionIpBlockingRule regionIpBlockingRule1 = mock(RegionIpBlockingRule.class);
    RegionIpBlockingRule regionIpBlockingRule2 = mock(RegionIpBlockingRule.class);
    List<RegionIpBlockingRule> mockRegionIpBlockingRules =
        List.of(regionIpBlockingRule1, regionIpBlockingRule2);

    doReturn(mockRegionIpBlockingRules)
        .when(mockAggregator)
        .getEnabledBlockingRules(requestContext, environmentId);
    doReturn("mock-hash").when(mockUuidGenerator).generateId(any(RegionBlockingRules.class));

    // Test in case hashes don't match the new region config is loaded
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setRegionBlockingRules(
                    RegionBlockingRules.newBuilder()
                        .addAllRegionIpBlockingRules(mockRegionIpBlockingRules))
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .build()),
            new BlockingRulesSupplier(Collections.emptyMap(), requestContext, environmentId)));

    // Test in case hashes do match the new region config is empty
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setRegionBlockingRules(RegionBlockingRules.getDefaultInstance())
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            new BlockingRulesSupplier(Collections.emptyMap(), requestContext, environmentId)));

    // Test empty in case request elements are empty
    assertEquals(
        List.of(),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            new BlockingRulesSupplier(Collections.emptyMap(), requestContext, environmentId)));
  }
}
