package ai.traceable.blocking.config.service.iptype;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.iptype.IpTypeConverter;
import ai.traceable.malicioussources.config.service.v1.EnvironmentScope;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockingIpTypesClientTest {
  private MockMaliciousSourcesConfigService mockMaliciousSourcesConfigService;
  private MaliciousSourcesConfigServiceBlockingStub maliciousSourcesConfigServiceBlockingStub;

  @BeforeEach
  void setup() {
    this.mockMaliciousSourcesConfigService = new MockMaliciousSourcesConfigService();
    this.mockMaliciousSourcesConfigService.start();
    this.maliciousSourcesConfigServiceBlockingStub =
        MaliciousSourcesConfigServiceGrpc.newBlockingStub(
            this.mockMaliciousSourcesConfigService.channel());
  }

  @AfterEach
  void tearDown() {
    this.mockMaliciousSourcesConfigService.shutdown();
  }

  @Test
  void getBlockingIpTypes() {
    BlockingIpTypesClient fetcher =
        new BlockingIpTypesClient(maliciousSourcesConfigServiceBlockingStub);
    mockMaliciousSourcesConfigService.addRule(
        "1",
        List.of(IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER),
        MaliciousSourcesRuleScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.getDefaultInstance())
            .build());
    mockMaliciousSourcesConfigService.addRule(
        "2",
        List.of(
            IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER,
            IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY),
        MaliciousSourcesRuleScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.getDefaultInstance())
            .build());
    mockMaliciousSourcesConfigService.addRule(
        "env-only-1",
        List.of(IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN),
        MaliciousSourcesRuleScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("env-1")).build())
            .build());
    mockMaliciousSourcesConfigService.addRule(
        "env-1-2",
        List.of(
            IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN,
            IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY),
        MaliciousSourcesRuleScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder()
                    .addAllEnvironmentIds(List.of("env-1", "env-2"))
                    .build())
            .build());
    mockMaliciousSourcesConfigService.addRule(
        "env-only-2",
        List.of(IpLocationType.IP_LOCATION_TYPE_BOT),
        MaliciousSourcesRuleScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("env-2")).build())
            .build());

    List<IpType> ipTypes =
        fetcher
            .fetchMaliciousSourceRules(RequestContext.forTenantId("tenantId"), Optional.empty())
            .stream()
            .flatMap(
                maliciousSourcesRule ->
                    BlockingIpTypesClient.getBlockingIpTypes(maliciousSourcesRule).stream())
            .distinct()
            .map(IpTypeConverter::getIpType)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        Set.of(IpType.IP_TYPE_PROXY, IpType.IP_TYPE_HOSTING_PROVIDER), new HashSet<>(ipTypes));

    ipTypes =
        fetcher
            .fetchMaliciousSourceRules(RequestContext.forTenantId("tenantId"), Optional.of("env-1"))
            .stream()
            .flatMap(
                maliciousSourcesRule ->
                    BlockingIpTypesClient.getBlockingIpTypes(maliciousSourcesRule).stream())
            .distinct()
            .map(IpTypeConverter::getIpType)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        Set.of(IpType.IP_TYPE_VPN, IpType.IP_TYPE_PROXY, IpType.IP_TYPE_HOSTING_PROVIDER),
        new HashSet<>(ipTypes));

    ipTypes =
        fetcher
            .fetchMaliciousSourceRules(RequestContext.forTenantId("tenantId"), Optional.of("env-2"))
            .stream()
            .flatMap(
                maliciousSourcesRule ->
                    BlockingIpTypesClient.getBlockingIpTypes(maliciousSourcesRule).stream())
            .distinct()
            .map(IpTypeConverter::getIpType)
            .collect(Collectors.toUnmodifiableList());
    assertEquals(
        Set.of(
            IpType.IP_TYPE_BOT,
            IpType.IP_TYPE_VPN,
            IpType.IP_TYPE_PROXY,
            IpType.IP_TYPE_HOSTING_PROVIDER),
        new HashSet<>(ipTypes));
  }
}
