package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.v1.BlockingInfo;
import ai.traceable.blocking.config.service.v1.BlockingRule;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import ai.traceable.blocking.config.service.v1.IpBlocking;
import ai.traceable.blocking.config.service.v1.RuleActionType;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionsRequest;
import ai.traceable.region.config.service.v1.Region;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.RegionsFilter;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static RegionConfigServiceBlockingStub regionConfigServiceStub;
  private static BlockingConfigServiceBlockingStub blockingConfigServiceStub;

  @BeforeAll
  static void init() {
    regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    blockingConfigServiceStub =
        BlockingConfigServiceGrpc.newBlockingStub(managedChannelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void getBlockingRules() {
    List<Region> regions =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .getRegions(GetRegionsRequest.getDefaultInstance())
                    .getRegionList());

    List<DetailedRegion> detailedRegions =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    regionConfigServiceStub.getDetailedRegions(
                        GetDetailedRegionsRequest.newBuilder()
                            .setFilter(
                                RegionsFilter.newBuilder()
                                    .addId(regions.get(0).getId())
                                    .addId(regions.get(1).getId())
                                    .addId(regions.get(2).getId())
                                    .build())
                            .build()))
            .getRegionList();

    DetailedRegion region1 = detailedRegions.get(0);
    DetailedRegion region2 = detailedRegions.get(1);
    DetailedRegion region3 = detailedRegions.get(2);

    // creating region rules
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            regionConfigServiceStub.createRegionRule(
                CreateRegionRuleRequest.newBuilder()
                    .setName("rule-1")
                    .addRegionId(region1.getId())
                    .addRegionId(region2.getId())
                    .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                    .build()));

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            regionConfigServiceStub.createRegionRule(
                CreateRegionRuleRequest.newBuilder()
                    .setName("rule-2")
                    .addRegionId(region3.getId())
                    .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                    .build()));

    GetBlockingRulesResponse response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                blockingConfigServiceStub.getBlockingRules(
                    GetBlockingRulesRequest.getDefaultInstance()));
    List<BlockingRule> blockingRules = response.getRuleList();
    String hash = response.getHash();

    assertEquals(2, blockingRules.size());

    BlockingRule blockingRule1 = blockingRules.get(0);
    assertEquals(RuleActionType.RULE_ACTION_TYPE_BLOCK, blockingRule1.getActionType());
    BlockingInfo blockingInfo1 = blockingRule1.getBlockingInfo();
    assertEquals(0, blockingInfo1.getExpirationMillis());
    assertEquals("CUSTOM_REGION_RULE", blockingInfo1.getCategory());
    assertFalse(blockingInfo1.getInfo().isEmpty());
    IpBlocking ipBlocking1 = blockingRule1.getIpBlocking();
    assertEquals(region3.getIpRangeCount(), ipBlocking1.getIpRangeCount());

    BlockingRule blockingRule2 = blockingRules.get(1);
    assertEquals(RuleActionType.RULE_ACTION_TYPE_BLOCK, blockingRule2.getActionType());
    BlockingInfo blockingInfo2 = blockingRule2.getBlockingInfo();
    assertEquals(0, blockingInfo2.getExpirationMillis());
    assertEquals("CUSTOM_REGION_RULE", blockingInfo2.getCategory());
    assertFalse(blockingInfo2.getInfo().isEmpty());
    IpBlocking ipBlocking2 = blockingRule2.getIpBlocking();
    assertEquals(
        region1.getIpRangeCount() + region2.getIpRangeCount(), ipBlocking2.getIpRangeCount());

    // querying again with the same hash shouldn't return blocking rules
    GetBlockingRulesResponse sameHashedResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                blockingConfigServiceStub.getBlockingRules(
                    GetBlockingRulesRequest.newBuilder().setOmitIfMatchesHash(hash).build()));

    assertEquals(hash, sameHashedResponse.getHash());
    assertTrue(sameHashedResponse.getRuleList().isEmpty());
  }

  @Test
  void getBlockingRules_changeIfUpdateInRegionRules() {
    List<Region> regions =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .getRegions(GetRegionsRequest.getDefaultInstance())
                    .getRegionList());

    List<DetailedRegion> detailedRegions =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    regionConfigServiceStub.getDetailedRegions(
                        GetDetailedRegionsRequest.newBuilder()
                            .setFilter(
                                RegionsFilter.newBuilder()
                                    .addId(regions.get(0).getId())
                                    .addId(regions.get(1).getId())
                                    .addId(regions.get(2).getId())
                                    .build())
                            .build()))
            .getRegionList();

    DetailedRegion region1 = detailedRegions.get(0);
    DetailedRegion region2 = detailedRegions.get(1);
    DetailedRegion region3 = detailedRegions.get(2);

    // creating region rules
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            regionConfigServiceStub.createRegionRule(
                CreateRegionRuleRequest.newBuilder()
                    .setName("rule-1")
                    .addRegionId(region1.getId())
                    .addRegionId(region2.getId())
                    .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                    .build()));

    RegionRule regionRule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .setName("rule-2")
                            .addRegionId(region3.getId())
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                            .build())
                    .getRule());

    GetBlockingRulesResponse response1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                blockingConfigServiceStub.getBlockingRules(
                    GetBlockingRulesRequest.getDefaultInstance()));

    List<BlockingRule> blockingRules1 = response1.getRuleList();
    String hash1 = response1.getHash();

    long expirationMillis = System.currentTimeMillis() + 10000;
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            regionConfigServiceStub.updateRegionRule(
                UpdateRegionRuleRequest.newBuilder()
                    .setRule(
                        RegionRule.newBuilder(regionRule2)
                            .setExpirationMillis(expirationMillis)
                            .build())
                    .build()));

    GetBlockingRulesResponse response2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                blockingConfigServiceStub.getBlockingRules(
                    GetBlockingRulesRequest.newBuilder().setOmitIfMatchesHash(hash1).build()));

    List<BlockingRule> blockingRules2 = response2.getRuleList();
    String hash2 = response2.getHash();
    assertNotEquals(hash1, hash2);
    assertNotEquals(blockingRules1, blockingRules2);

    assertEquals(2, blockingRules2.size());

    BlockingRule blockingRule1 = blockingRules2.get(0);
    assertEquals(RuleActionType.RULE_ACTION_TYPE_BLOCK, blockingRule1.getActionType());
    BlockingInfo blockingInfo1 = blockingRule1.getBlockingInfo();
    assertEquals(expirationMillis, blockingInfo1.getExpirationMillis());
    assertEquals("CUSTOM_REGION_RULE", blockingInfo1.getCategory());
    assertFalse(blockingInfo1.getInfo().isEmpty());
    IpBlocking ipBlocking1 = blockingRule1.getIpBlocking();
    assertEquals(region3.getIpRangeCount(), ipBlocking1.getIpRangeCount());

    BlockingRule blockingRule2 = blockingRules2.get(1);
    assertEquals(RuleActionType.RULE_ACTION_TYPE_BLOCK, blockingRule2.getActionType());
    BlockingInfo blockingInfo2 = blockingRule2.getBlockingInfo();
    assertEquals(0, blockingInfo2.getExpirationMillis());
    assertEquals("CUSTOM_REGION_RULE", blockingInfo2.getCategory());
    assertFalse(blockingInfo2.getInfo().isEmpty());
    IpBlocking ipBlocking2 = blockingRule2.getIpBlocking();
    assertEquals(
        region1.getIpRangeCount() + region2.getIpRangeCount(), ipBlocking2.getIpRangeCount());
  }
}
