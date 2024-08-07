package ai.traceable.blocking.config.service.common.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo.IpType;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRulesLoader;
import ai.traceable.blocking.config.service.common.rules.fetchers.CustomSignatureRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.DlpRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.ExclusionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.MaliciousSourcesRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RegionRulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher;
import ai.traceable.blocking.config.service.common.rules.fetchers.RulesFetcher.RulesFetcherType;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
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
import ai.traceable.region.config.service.v1.RegionRule;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BlockingRulesSupplierTest {

  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenantId");
  private static final Optional<String> ENVIRONMENT_ID = Optional.of("env");
  private static final Set<String> SERVICE_NAMES =
      new LinkedHashSet<>(List.of("service1", "service2", "service-x"));
  private static final Map<String, RateLimitingModsecRule> rateLimitingModsecRulesMap =
      createRateLimitingModsecRuleMap();
  private static final Map<String, DetectionExclusionModsecRule> detectionExclusionRuleMap =
      createDetectionExclusionRuleMap();

  BlockingRulesSupplierContext blockingRulesSupplierContext;

  RegionRulesFetcher regionRulesFetcher;
  CustomSignatureRulesFetcher customSignatureRulesFetcher;
  MaliciousSourcesRulesFetcher maliciousSourcesRulesFetcher;
  DlpRulesFetcher dlpRulesFetcher;
  ExclusionRulesFetcher exclusionRulesFetcher;

  Map<RulesFetcher.RulesFetcherType, RulesFetcher> rulesFetchers;

  BlockingRulesSupplier blockingRulesSupplier;

  @BeforeEach
  public void setup() {
    regionRulesFetcher = mock(RegionRulesFetcher.class);
    customSignatureRulesFetcher = mock(CustomSignatureRulesFetcher.class);
    maliciousSourcesRulesFetcher = mock(MaliciousSourcesRulesFetcher.class);
    dlpRulesFetcher = mock(DlpRulesFetcher.class);
    exclusionRulesFetcher = mock(ExclusionRulesFetcher.class);

    rulesFetchers =
        Map.of(
            RulesFetcher.RulesFetcherType.CUSTOM_SIGNATURE,
            customSignatureRulesFetcher,
            RulesFetcher.RulesFetcherType.REGION,
            regionRulesFetcher,
            RulesFetcher.RulesFetcherType.MALICIOUS_SOURCES,
            maliciousSourcesRulesFetcher,
            RulesFetcher.RulesFetcherType.DLP,
            dlpRulesFetcher,
            RulesFetcherType.EXCLUSION,
            exclusionRulesFetcher);

    IpTypeRulesLoader ipTypeRulesLoader = mock(IpTypeRulesLoader.class);
    when(ipTypeRulesLoader.getLatestDataSupplier())
        .thenReturn(
            () ->
                Arrays.stream(IpTypeRuleInfo.IpType.values())
                    .collect(
                        Collectors.toMap(
                            Function.identity(),
                            ipType -> IpTypeRuleInfo.builder().ipType(ipType).build())));

    when(dlpRulesFetcher.fetchDlpModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, SERVICE_NAMES))
        .thenReturn(
            Map.of(
                "service1",
                new ModsecRulesData<>(
                    "directives",
                    "blobA",
                    List.of("id1", "id2"),
                    rateLimitingModsecRulesMap.values(),
                    RateLimitingModsecRule::getId),
                "service2",
                new ModsecRulesData<>(
                    "directives",
                    "blobB",
                    List.of("id1", "id3"),
                    rateLimitingModsecRulesMap.values(),
                    RateLimitingModsecRule::getId),
                "service-x",
                new ModsecRulesData<>()));

    when(exclusionRulesFetcher.fetchExclusionModsecRules(
            REQUEST_CONTEXT, ENVIRONMENT_ID, SERVICE_NAMES))
        .thenReturn(
            Map.of(
                "service1",
                new ModsecRulesData<>(
                    "directives",
                    "blobA0",
                    List.of("id10", "id20"),
                    detectionExclusionRuleMap.values(),
                    rule -> rule.getRule().getId()),
                "service-x",
                new ModsecRulesData<>()));

    blockingRulesSupplierContext =
        new BlockingRulesSupplierContext(rulesFetchers, ipTypeRulesLoader);
  }

  @Test
  public void test_getCustomSignatureRulesBlob0() {
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
    modsecBlobs = blockingRulesSupplier.getCustomModsecBlobs(version1, SERVICE_NAMES);
    assertEquals(3, modsecBlobs.size());
    for (String sName : SERVICE_NAMES) {
      assertEquals(blob1, modsecBlobs.get(sName));
    }
    verify(customSignatureRulesFetcher, times(1)).fetchModsecRules(any(), any(), any());
    verify(dlpRulesFetcher, times(0)).fetchDlpModsecRules(any(), any(), any());
    verify(exclusionRulesFetcher, times(0)).fetchExclusionModsecRules(any(), any(), any());

    modsecBlobs = blockingRulesSupplier.getCustomModsecBlobs(version2, SERVICE_NAMES);
    assertEquals(3, modsecBlobs.size());
    // no dlp rule for service-x
    assertEquals(blob2, modsecBlobs.get("service-x"));
    assertEquals(blob2 + "\nblobA" + "\nblobA0", modsecBlobs.get("service1"));
    assertEquals(blob2 + "\nblobB", modsecBlobs.get("service2"));
    verify(customSignatureRulesFetcher, times(2)).fetchModsecRules(any(), any(), any());
    verify(dlpRulesFetcher, times(1)).fetchDlpModsecRules(any(), any(), eq(SERVICE_NAMES));
    verify(exclusionRulesFetcher, times(1))
        .fetchExclusionModsecRules(any(), any(), eq(SERVICE_NAMES));

    modsecBlobs =
        blockingRulesSupplier.getCustomModsecBlobs(
            version2, new LinkedHashSet<>(List.of("service1", "service-x")));
    assertEquals(2, modsecBlobs.size());
    // no dlp or exclusion rule for service-x
    assertEquals(blob2, modsecBlobs.get("service-x"));
    assertEquals(blob2 + "\nblobA" + "\nblobA0", modsecBlobs.get("service1"));
    // fetchModsecRules will not be called again.
    verify(customSignatureRulesFetcher, times(2)).fetchModsecRules(any(), any(), any());
    // fetchDlpModsecRules will not be called again for the list of services.
    verify(dlpRulesFetcher, times(2)).fetchDlpModsecRules(any(), any(), any());
    verify(dlpRulesFetcher, times(1)).fetchDlpModsecRules(any(), any(), eq(SERVICE_NAMES));
    verify(dlpRulesFetcher, times(1)).fetchDlpModsecRules(any(), any(), eq(Set.of()));

    verify(exclusionRulesFetcher, times(2)).fetchExclusionModsecRules(any(), any(), any());
    verify(exclusionRulesFetcher, times(1))
        .fetchExclusionModsecRules(any(), any(), eq(SERVICE_NAMES));
    verify(exclusionRulesFetcher, times(1)).fetchExclusionModsecRules(any(), any(), eq(Set.of()));

    blockingRulesSupplier =
        new BlockingRulesSupplier(blockingRulesSupplierContext, REQUEST_CONTEXT, ENVIRONMENT_ID);
    when(customSignatureRulesFetcher.fetchModsecRules(REQUEST_CONTEXT, ENVIRONMENT_ID, version2))
        .thenReturn(GetCustomSignatureModsecRulesResponse.getDefaultInstance());
    modsecBlobs = blockingRulesSupplier.getCustomModsecBlobs(version2, SERVICE_NAMES);
    assertEquals(3, modsecBlobs.size());
    // no dlp rule for service-x
    assertEquals("", modsecBlobs.get("service-x"));
    assertEquals("directives\nblobA\nblobA0", modsecBlobs.get("service1"));
    assertEquals("directives\nblobB", modsecBlobs.get("service2"));
  }

  @Test
  public void test_getRegionIpMappings() {
    final DetailedRegion detailedRegion1 = getDetailedRegion("id1", "isoCode1");
    final DetailedRegion detailedRegion2 = getDetailedRegion("id2", "isoCode2");
    final DetailedRegion detailedRegion3 = getDetailedRegion("id3", "isoCode3");
    final DetailedRegion detailedRegion4 = getDetailedRegion("id4", "isoCode4");
    final DetailedRegion detailedRegion5 = getDetailedRegion("id5", "isoCode5");

    when(regionRulesFetcher.fetchRegionRules(REQUEST_CONTEXT, ENVIRONMENT_ID))
        .thenReturn(
            List.of(
                buildRegionRules("id1", "isoCode1"),
                buildRegionRules("id2", "isoCode2"),
                buildRegionRules("id3", "isoCode3")));
    when(regionRulesFetcher.fetchDetailedRegions(List.of("isoCode1", "isoCode2", "isoCode3")))
        .thenReturn(List.of(detailedRegion1, detailedRegion2, detailedRegion3));
    when(regionRulesFetcher.fetchDetailedRegions(
            List.of("isoCode1", "isoCode2", "isoCode3", "isoCode4", "isoCode5")))
        .thenReturn(
            List.of(
                detailedRegion1,
                detailedRegion2,
                detailedRegion3,
                detailedRegion4,
                detailedRegion5));

    blockingRulesSupplier =
        new BlockingRulesSupplier(blockingRulesSupplierContext, REQUEST_CONTEXT, ENVIRONMENT_ID);
    assertEquals(
        List.of(detailedRegion1, detailedRegion2, detailedRegion3),
        blockingRulesSupplier.getRegionIpMappings(Function.identity(), Collections.emptySet()));
    verify(regionRulesFetcher, times(1)).fetchRegionRules(any(), any());
    assertEquals(
        List.of(detailedRegion1, detailedRegion2, detailedRegion3),
        blockingRulesSupplier.getRegionIpMappings(Function.identity(), Collections.emptySet()));
    // fetchRules will not be called again..
    verify(regionRulesFetcher, times(1)).fetchRegionRules(any(), any());

    assertEquals(
        List.of(
            detailedRegion1, detailedRegion2, detailedRegion3, detailedRegion4, detailedRegion5),
        blockingRulesSupplier.getRegionIpMappings(Function.identity(), SERVICE_NAMES));
    // fetchRules will not be called again.
    verify(regionRulesFetcher, times(1)).fetchRegionRules(any(), any());
    verify(dlpRulesFetcher, times(1)).fetchDlpModsecRules(any(), any(), eq(SERVICE_NAMES));
    verify(exclusionRulesFetcher, times(1))
        .fetchExclusionModsecRules(any(), any(), eq(SERVICE_NAMES));
  }

  private static RegionRule buildRegionRules(String regionId, String isoCode) {
    return RegionRule.newBuilder()
        .setId("id")
        .addRegionId(regionId)
        .putRegionIdToCountryMap(regionId, Country.newBuilder().setIsoCode(isoCode).build())
        .build();
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

    List<IpTypeRuleInfo.IpType> ipTypeRuleInfoList =
        blockingRulesSupplier
            .getIpTypeIpMappings(t -> new ImmutablePair<>(t, ""), Collections.emptySet())
            .stream()
            .map(ImmutablePair::getLeft)
            .map(IpTypeRuleInfo::getIpType)
            .collect(Collectors.toList());
    verify(maliciousSourcesRulesFetcher, times(1)).fetchRules(any(), any());
    assertEquals(
        List.of(
            IpTypeRuleInfo.IpType.BOT,
            IpTypeRuleInfo.IpType.ANONYMOUS_VPN,
            IpTypeRuleInfo.IpType.PUBLIC_PROXY),
        ipTypeRuleInfoList);

    ipTypeRuleInfoList =
        blockingRulesSupplier
            .getIpTypeIpMappings(t -> new ImmutablePair<>(t, ""), SERVICE_NAMES)
            .stream()
            .map(ImmutablePair::getLeft)
            .map(IpTypeRuleInfo::getIpType)
            .collect(Collectors.toList());
    assertEquals(
        List.of(
            IpTypeRuleInfo.IpType.BOT,
            IpTypeRuleInfo.IpType.ANONYMOUS_VPN,
            IpTypeRuleInfo.IpType.PUBLIC_PROXY,
            IpTypeRuleInfo.IpType.HOSTING_PROVIDER,
            IpType.TOR_EXIT_NODE),
        ipTypeRuleInfoList);
    verify(dlpRulesFetcher, times(1)).fetchDlpModsecRules(any(), any(), eq(SERVICE_NAMES));
    verify(exclusionRulesFetcher, times(1))
        .fetchExclusionModsecRules(any(), any(), eq(SERVICE_NAMES));
    // fetchRules will not be called again.
    verify(maliciousSourcesRulesFetcher, times(1)).fetchRules(any(), any());
  }

  @Test
  public void test_getDlpRules() {
    blockingRulesSupplier =
        new BlockingRulesSupplier(blockingRulesSupplierContext, REQUEST_CONTEXT, ENVIRONMENT_ID);
    assertTrue(blockingRulesSupplier.getDlpRules(Collections.emptySet()).isEmpty());

    Map<String, List<RateLimitingModsecRule>> dlpRules =
        blockingRulesSupplier.getDlpRules(SERVICE_NAMES);
    assertEquals(3, dlpRules.size());
    assertEquals(
        List.of(rateLimitingModsecRulesMap.get("id1"), rateLimitingModsecRulesMap.get("id2")),
        dlpRules.get("service1"));
    assertEquals(
        List.of(rateLimitingModsecRulesMap.get("id1"), rateLimitingModsecRulesMap.get("id3")),
        dlpRules.get("service2"));
    assertTrue(dlpRules.get("service-x").isEmpty());
  }

  @Test
  public void test_getExclusionRules() {
    blockingRulesSupplier =
        new BlockingRulesSupplier(blockingRulesSupplierContext, REQUEST_CONTEXT, ENVIRONMENT_ID);
    assertTrue(blockingRulesSupplier.getExclusionRules(Collections.emptySet()).isEmpty());

    Map<String, List<DetectionExclusionModsecRule>> exclusionRules =
        blockingRulesSupplier.getExclusionRules(SERVICE_NAMES);
    assertEquals(3, exclusionRules.size());
    assertEquals(
        List.of(detectionExclusionRuleMap.get("id10"), detectionExclusionRuleMap.get("id20")),
        exclusionRules.get("service1"));
    assertTrue(exclusionRules.get("service-x").isEmpty());
  }

  private static Map<String, RateLimitingModsecRule> createRateLimitingModsecRuleMap() {
    Map<String, RateLimitingModsecRule> rulesMap = new LinkedHashMap<>();
    rulesMap.put(
        "id1",
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
                                                                RegionCondition.Region.newBuilder()
                                                                    .setCountryIsoCode(
                                                                        "isoCode4")))))
                                    .addChildren(
                                        Condition.newBuilder()
                                            .setLeafCondition(
                                                LeafCondition.newBuilder()
                                                    .setIpLocationTypeCondition(
                                                        ai.traceable.ratelimiting.config.service.v2
                                                            .IpLocationTypeCondition.newBuilder()
                                                            .addIpLocationTypes(
                                                                ai.traceable.ratelimiting.config
                                                                    .service.v2.IpLocationType
                                                                    .IP_LOCATION_TYPE_HOSTING_PROVIDER)))))))
            .build());
    rulesMap.put(
        "id2",
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
            .build());
    rulesMap.put(
        "id3",
        RateLimitingModsecRule.newBuilder()
            .setId("id3")
            .setData(
                RateLimitingRuleData.newBuilder()
                    .setCondition(
                        Condition.newBuilder()
                            .setLeafCondition(
                                LeafCondition.newBuilder()
                                    .setIpLocationTypeCondition(
                                        ai.traceable.ratelimiting.config.service.v2
                                            .IpLocationTypeCondition.newBuilder()
                                            .addIpLocationTypes(
                                                ai.traceable.ratelimiting.config.service.v2
                                                    .IpLocationType
                                                    .IP_LOCATION_TYPE_PUBLIC_PROXY)))))
            .build());
    rulesMap.put("id4", RateLimitingModsecRule.newBuilder().setId("id4").build());
    return rulesMap;
  }

  private static Map<String, DetectionExclusionModsecRule> createDetectionExclusionRuleMap() {
    Map<String, DetectionExclusionModsecRule> rulesMap = new LinkedHashMap<>();
    rulesMap.put(
        "id10",
        DetectionExclusionModsecRule.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setId("id10")
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setRegionCondition(
                                        ai.traceable.detection.exclusion.config.service.v1
                                            .RegionCondition.newBuilder()
                                            .addRegions(
                                                ai.traceable.detection.exclusion.config.service.v1
                                                    .RegionCondition.Region.newBuilder()
                                                    .setCountryIsoCode("isoCode5"))))
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setIpLocationTypeCondition(
                                        ai.traceable.detection.exclusion.config.service.v1
                                            .IpLocationTypeCondition.newBuilder()
                                            .addIpLocationTypes(
                                                ai.traceable.detection.exclusion.config.service.v1
                                                    .IpLocationType
                                                    .IP_LOCATION_TYPE_TOR_EXIT_NODE)))))
            .build());

    rulesMap.put(
        "id20",
        DetectionExclusionModsecRule.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setId("id20")
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setRegionCondition(
                                        ai.traceable.detection.exclusion.config.service.v1
                                            .RegionCondition.newBuilder()
                                            .addRegions(
                                                ai.traceable.detection.exclusion.config.service.v1
                                                    .RegionCondition.Region.newBuilder()
                                                    .setCountryIsoCode("isoCode2"))))))
            .build());
    rulesMap.put(
        "id40",
        DetectionExclusionModsecRule.newBuilder()
            .setRule(DetectionExclusionRule.newBuilder().setId("id40"))
            .build());
    return rulesMap;
  }
}
