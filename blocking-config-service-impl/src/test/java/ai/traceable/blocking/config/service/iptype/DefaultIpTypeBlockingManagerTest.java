package ai.traceable.blocking.config.service.iptype;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.v1.IpRange;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.blocking.config.service.v1.IpV4Range;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.config.utils.refresh.FileRefreshConfig;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class DefaultIpTypeBlockingManagerTest {

  @Test
  void getEnabledBlockingRules() {
    FileRefreshConfig config = mock(FileRefreshConfig.class);
    IpTypeRulesLoader ipTypeRulesLoader = mock(IpTypeRulesLoader.class);
    BlockingIpTypesClient blockingIpTypesClient = mock(BlockingIpTypesClient.class);

    List<IpTypeRule> ipTypeRuleList =
        List.of(
            IpTypeRule.newBuilder()
                .setIpType(IpType.IP_TYPE_BOT)
                .addIpRanges(
                    IpRange.newBuilder()
                        .setIpv4Range(IpV4Range.newBuilder().setStartIp(1).setEndIp(3).build()))
                .build());
    Supplier<List<IpTypeRule>> ipTypeRuleSupplier = () -> ipTypeRuleList;

    when(blockingIpTypesClient.getBlockingIpTypes(any(), any(), any()))
        .thenReturn(List.of(IpType.IP_TYPE_BOT, IpType.IP_TYPE_HOSTING_PROVIDER));
    when(ipTypeRulesLoader.getLatestDataSupplier(any())).thenReturn(ipTypeRuleSupplier);

    DefaultIpTypeBlockingManager manager =
        new DefaultIpTypeBlockingManager(
            config,
            ipTypeRulesLoader,
            mock(MaliciousSourcesConfigServiceBlockingStub.class),
            new UuidGenerator(),
            blockingIpTypesClient);

    IpTypeBlockingRules rules =
        manager.getEnabledBlockingRules(
            RequestContext.forTenantId("tenantId"), "", Optional.empty());
    String requestHash = rules.getHash();
    assertEquals(ipTypeRuleList, rules.getIpTypeRuleListList());

    rules =
        manager.getEnabledBlockingRules(
            RequestContext.forTenantId("tenantId"), requestHash, Optional.empty());
    assertEquals(0, rules.getIpTypeRuleListList().size());
    assertEquals(requestHash, rules.getHash());
  }
}
