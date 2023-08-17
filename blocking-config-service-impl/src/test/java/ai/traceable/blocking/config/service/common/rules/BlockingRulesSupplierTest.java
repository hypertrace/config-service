package ai.traceable.blocking.config.service.common.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRulesLoader;
import ai.traceable.blocking.config.service.common.rules.fetchers.CustomSignatureRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.DlpRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.MaliciousSourcesRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RegionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.malicioussources.config.service.v1.IpAddressCondition;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.region.config.service.v1.Country;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.Region;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BlockingRulesSupplierTest {

  private static final RequestContext REQUEST_CONTEXT = mock(RequestContext.class);
  private static final Optional<String> ENVIRONMENT_ID = Optional.of("env");
  private static List<String> SERVICE_NAMES = List.of("service1", "service2", "service-x");

  BlockingRulesSupplierContext blockingRulesSupplierContext;

  RegionRulesFetcher regionRulesFetcher;
  CustomSignatureRulesFetcher customSignatureRulesFetcher;
  MaliciousSourcesRulesFetcher maliciousSourcesRulesFetcher;
  DlpRulesFetcher dlpRulesFetcher;

  Map<RulesFetcher.RulesFetcherType, RulesFetcher> rulesFetchers;

  BlockingRulesSupplier blockingRulesSupplier;

  @BeforeEach
  public void setup() {
    regionRulesFetcher = mock(RegionRulesFetcher.class);
    customSignatureRulesFetcher = mock(CustomSignatureRulesFetcher.class);
    maliciousSourcesRulesFetcher = mock(MaliciousSourcesRulesFetcher.class);
    dlpRulesFetcher = mock(DlpRulesFetcher.class);

    rulesFetchers =
        Map.of(
            RulesFetcher.RulesFetcherType.CUSTOM_SIGNATURE,
            customSignatureRulesFetcher,
            RulesFetcher.RulesFetcherType.REGION,
            regionRulesFetcher,
            RulesFetcher.RulesFetcherType.MALICIOUS_SOURCES,
            maliciousSourcesRulesFetcher,
            RulesFetcher.RulesFetcherType.DLP,
            dlpRulesFetcher);

    IpTypeRulesLoader ipTypeRulesLoader = mock(IpTypeRulesLoader.class);
    when(ipTypeRulesLoader.getLatestDataSupplier())
        .thenReturn(
            () ->
                Arrays.stream(IpLocationType.values())
                    .filter(ipType -> !ipType.equals(IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE))
                    .collect(
                        Collectors.toMap(
                            Function.identity(), ipType -> new IpTypeRuleInfo(ipType))));

    List<RateLimitingModsecRule> rateLimitingModsecRules =
        List.of(
            RateLimitingModsecRule.newBuilder()
                .setId("id1")
                .setData(
                    RateLimitingRuleData.newBuilder()
                        .setCondition(
                            Condition.newBuilder()
                                .setCompositeCondition(
                                    CompositeCondition.newBuilder()
                                        .addChildren(
                                            Condition.newBuilder()
                                                .setLeafCondition(
                                                    LeafCondition.newBuilder()
                                                        .setRegionCondition(
                                                            RegionCondition.newBuilder()
                                                                .addRegionIdentifiers(
                                                                    RegionCondition.Region
                                                                        .newBuilder()
                                                                        .setCountryIsoCode(
                                                                            "isoCode4"))))))))
                .build(),
            RateLimitingModsecRule.newBuilder()
                .setId("id2")
                .setData(
                    RateLimitingRuleData.newBuilder()
                        .setCondition(
                            Condition.newBuilder()
                                .setLeafCondition(
                                    LeafCondition.newBuilder()
                                        .setRegionCondition(
                                            RegionCondition.newBuilder()
                                                .addRegionIdentifiers(
                                                    RegionCondition.Region.newBuilder()
                                                        .setCountryIsoCode("isoCode2"))))))
                .build(),
            RateLimitingModsecRule.newBuilder().setId("id3").build());

    when(dlpRulesFetcher.fetchDlpModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, SERVICE_NAMES))
        .thenReturn(
            Map.of(
                "service1",
                new DlpRulesFetcher.DlpModsecRulesData(
                    "directives", "blobA", List.of("id1", "id2"), rateLimitingModsecRules),
                "service2",
                new DlpRulesFetcher.DlpModsecRulesData(
                    "directives", "blobB", List.of("id1", "id3"), rateLimitingModsecRules),
                "service-x",
                new DlpRulesFetcher.DlpModsecRulesData()));

    blockingRulesSupplierContext =
        new BlockingRulesSupplierContext(rulesFetchers, ipTypeRulesLoader);
  }

  @Test
  public void test_getCustomSignatureRulesBlob() {
    CustomModsecRuleVersion version1 = CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;
    CustomModsecRuleVersion version2 =
        CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS;
    String blob1 = "blob1";
    String blob2 = "blob2";

    when(customSignatureRulesFetcher.fetchModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, version1))
        .thenReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder().setModsecRulesBlob(blob1).build());
    when(customSignatureRulesFetcher.fetchModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, version2))
        .thenReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder().setModsecRulesBlob(blob2).build());

    blockingRulesSupplier =
        new BlockingRulesSupplier(blockingRulesSupplierContext, REQUEST_CONTEXT, ENVIRONMENT_ID);
    assertEquals(blob1, blockingRulesSupplier.getCustomSignatureModsecBlob(version1));
    verify(customSignatureRulesFetcher, times(1)).fetchModsecRules(any(), any(), any());
    assertEquals(blob2, blockingRulesSupplier.getCustomSignatureModsecBlob(version2));
    verify(customSignatureRulesFetcher, times(2)).fetchModsecRules(any(), any(), any());
    assertEquals(blob2, blockingRulesSupplier.getCustomSignatureModsecBlob(version2));
    // fetchRules will not be called again..
    verify(customSignatureRulesFetcher, times(2)).fetchModsecRules(any(), any(), any());

    assertEquals(
        "",
        blockingRulesSupplier.getCustomSignatureModsecBlob(
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3));
  }

  @Test
  public void test_getCustomSignatureRulesBlobs() {
    CustomModsecRuleVersion version1 = CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;
    CustomModsecRuleVersion version2 =
        CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS_DETECTION_ONLY_MODE;

    String blob1 = "blob1";
    String blob2 = "blob2";

    when(customSignatureRulesFetcher.fetchModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, version1))
        .thenReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder().setModsecRulesBlob(blob1).build());
    when(customSignatureRulesFetcher.fetchModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, version2))
        .thenReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder().setModsecRulesBlob(blob2).build());

    blockingRulesSupplier =
        new BlockingRulesSupplier(blockingRulesSupplierContext, REQUEST_CONTEXT, ENVIRONMENT_ID);
    Map<String, String> modsecBlobs;

    // no dlp rules fetched..
    modsecBlobs = blockingRulesSupplier.getCustomSignatureModsecBlobs(version1, SERVICE_NAMES);
    assertEquals(3, modsecBlobs.size());
    for (String sName : SERVICE_NAMES) {
      assertEquals(blob1, modsecBlobs.get(sName));
    }
    verify(customSignatureRulesFetcher, times(1)).fetchModsecRules(any(), any(), any());
    verify(dlpRulesFetcher, times(0)).fetchDlpModsecRules(any(), any(), any());

    modsecBlobs = blockingRulesSupplier.getCustomSignatureModsecBlobs(version2, SERVICE_NAMES);
    assertEquals(3, modsecBlobs.size());
    // no dlp rule for service-x
    assertEquals(blob2, modsecBlobs.get("service-x"));
    assertEquals(blob2 + "\nblobA", modsecBlobs.get("service1"));
    assertEquals(blob2 + "\nblobB", modsecBlobs.get("service2"));
    verify(customSignatureRulesFetcher, times(2)).fetchModsecRules(any(), any(), any());
    verify(dlpRulesFetcher, times(1)).fetchDlpModsecRules(any(), any(), eq(SERVICE_NAMES));

    modsecBlobs =
        blockingRulesSupplier.getCustomSignatureModsecBlobs(
            version2, List.of("service1", "service-x"));
    assertEquals(2, modsecBlobs.size());
    // no dlp rule for service-x
    assertEquals(blob2, modsecBlobs.get("service-x"));
    assertEquals(blob2 + "\nblobA", modsecBlobs.get("service1"));
    // fetchModsecRules will not be called again.
    verify(customSignatureRulesFetcher, times(2)).fetchModsecRules(any(), any(), any());
    // fetchDlpModsecRules will not be called again for the list of services.
    verify(dlpRulesFetcher, times(2)).fetchDlpModsecRules(any(), any(), any());
    verify(dlpRulesFetcher, times(1)).fetchDlpModsecRules(any(), any(), eq(SERVICE_NAMES));
    verify(dlpRulesFetcher, times(1)).fetchDlpModsecRules(any(), any(), eq(List.of()));

    blockingRulesSupplier =
        new BlockingRulesSupplier(blockingRulesSupplierContext, REQUEST_CONTEXT, ENVIRONMENT_ID);
    when(customSignatureRulesFetcher.fetchModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, version2))
        .thenReturn(GetCustomSignatureModsecRulesResponse.getDefaultInstance());
    modsecBlobs = blockingRulesSupplier.getCustomSignatureModsecBlobs(version2, SERVICE_NAMES);
    assertEquals(3, modsecBlobs.size());
    // no dlp rule for service-x
    assertEquals("", modsecBlobs.get("service-x"));
    assertEquals("directives\nblobA", modsecBlobs.get("service1"));
    assertEquals("directives\nblobB", modsecBlobs.get("service2"));
  }

  @Test
  public void test_getRegionIpMappings() {
    final DetailedRegion detailedRegion1 = getDetailedRegion("id1", "isoCode1");
    final DetailedRegion detailedRegion2 = getDetailedRegion("id2", "isoCode2");
    final DetailedRegion detailedRegion3 = getDetailedRegion("id3", "isoCode3");
    final DetailedRegion detailedRegion4 = getDetailedRegion("id4", "isoCode4");
    when(regionRulesFetcher.fetchRegionRules(REQUEST_CONTEXT, ENVIRONMENT_ID))
        .thenReturn(List.of(detailedRegion1, detailedRegion2, detailedRegion3));
    when(regionRulesFetcher.fetchDetailedRegions(List.of("isoCode4")))
        .thenReturn(Map.of("isoCode4", detailedRegion4));

    blockingRulesSupplier =
        new BlockingRulesSupplier(blockingRulesSupplierContext, REQUEST_CONTEXT, ENVIRONMENT_ID);
    assertEquals(
        List.of(detailedRegion1, detailedRegion2, detailedRegion3),
        blockingRulesSupplier.getRegionIpMappings(Function.identity(), Collections.emptyList()));
    verify(regionRulesFetcher, times(1)).fetchRegionRules(any(), any());
    assertEquals(
        List.of(detailedRegion1, detailedRegion2, detailedRegion3),
        blockingRulesSupplier.getRegionIpMappings(Function.identity(), Collections.emptyList()));
    // fetchRules will not be called again..
    verify(regionRulesFetcher, times(1)).fetchRegionRules(any(), any());

    assertEquals(
        List.of(detailedRegion1, detailedRegion2, detailedRegion3, detailedRegion4),
        blockingRulesSupplier.getRegionIpMappings(Function.identity(), SERVICE_NAMES));
    // fetchRules will not be called again..
    verify(regionRulesFetcher, times(1)).fetchRegionRules(any(), any());
    // fetchDetailedRegions will be called exactly once
    verify(regionRulesFetcher, times(1)).fetchDetailedRegions(List.of("isoCode4"));
    verify(regionRulesFetcher, times(1)).fetchDetailedRegions(any());
  }

  private static DetailedRegion getDetailedRegion(String regionId, String isoCode) {
    return DetailedRegion.newBuilder()
        .setId(regionId)
        .setRegion(Region.newBuilder().setCountry(Country.newBuilder().setIsoCode(isoCode)))
        .build();
  }

  @Test
  public void test_getIpTypeIpMappings() {
    MaliciousSourcesRule rule1 =
        MaliciousSourcesRule.newBuilder()
            .setRuleInfo(
                MaliciousSourcesRuleInfo.newBuilder()
                    .addConditions(
                        MaliciousSourcesRuleCondition.newBuilder()
                            .setIpLocationTypeCondition(
                                IpLocationTypeCondition.newBuilder()
                                    .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_BOT)
                                    .addIpLocationTypes(
                                        IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN))))
            .build();
    MaliciousSourcesRule rule2 =
        MaliciousSourcesRule.newBuilder()
            .setRuleInfo(
                MaliciousSourcesRuleInfo.newBuilder()
                    .addConditions(
                        MaliciousSourcesRuleCondition.newBuilder()
                            .setIpRangeCondition(
                                IpAddressCondition.newBuilder().addIpAddresses("1.2.3.4")))
                    .addConditions(
                        MaliciousSourcesRuleCondition.newBuilder()
                            .setIpLocationTypeCondition(
                                IpLocationTypeCondition.newBuilder()
                                    .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_BOT)
                                    .addIpLocationTypes(
                                        IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE)
                                    .addIpLocationTypes(
                                        IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY))))
            .build();
    MaliciousSourcesRule rule3 =
        MaliciousSourcesRule.newBuilder()
            .setRuleInfo(
                MaliciousSourcesRuleInfo.newBuilder()
                    .addConditions(
                        MaliciousSourcesRuleCondition.newBuilder()
                            .setIpRangeCondition(
                                IpAddressCondition.newBuilder().addIpAddresses("1.2.3.4"))))
            .build();

    when(maliciousSourcesRulesFetcher.fetchRules(REQUEST_CONTEXT, ENVIRONMENT_ID))
        .thenReturn(List.of(rule1, rule2, rule3));

    blockingRulesSupplier =
        new BlockingRulesSupplier(blockingRulesSupplierContext, REQUEST_CONTEXT, ENVIRONMENT_ID);

    List<IpLocationType> ipTypeRuleInfoList =
        blockingRulesSupplier.getIpTypeIpMappings(Function.identity()).stream()
            .map(IpTypeRuleInfo::getIpLocationType)
            .collect(Collectors.toList());
    verify(maliciousSourcesRulesFetcher, times(1)).fetchRules(any(), any());
    assertEquals(
        List.of(
            IpLocationType.IP_LOCATION_TYPE_BOT,
            IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN,
            IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY),
        ipTypeRuleInfoList);
    ipTypeRuleInfoList =
        blockingRulesSupplier.getIpTypeIpMappings(Function.identity()).stream()
            .map(IpTypeRuleInfo::getIpLocationType)
            .collect(Collectors.toList());
    // fetchRules will not be called again..
    verify(maliciousSourcesRulesFetcher, times(1)).fetchRules(any(), any());
  }
}
