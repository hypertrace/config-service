package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetRegionRequest;
import ai.traceable.region.config.service.v1.GetRegionResponse;
import ai.traceable.region.config.service.v1.GetRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionsResponse;
import ai.traceable.region.config.service.v1.Region;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RegionConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static RegionConfigServiceBlockingStub regionConfigServiceStub;

  @BeforeAll
  static void init() {
    regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void getRegions() {
    GetRegionsResponse regionsResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () -> regionConfigServiceStub.getRegions(GetRegionsRequest.getDefaultInstance()));
    List<Region> regions = regionsResponse.getRegionList();
    assertEquals(248, regions.size());
    regions.forEach(
        region -> {
          assertFalse(region.getId().isEmpty());
          assertFalse(region.getName().isEmpty());
        });
  }

  @Test
  public void getRegion() {
    GetRegionsResponse regionsResponse =
        regionConfigServiceStub.getRegions(GetRegionsRequest.getDefaultInstance());
    List<Region> regions = regionsResponse.getRegionList();

    Region firstRegion = regions.get(0);
    String regionId = firstRegion.getId();

    GetRegionResponse regionResponse =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub.getRegion(
                    GetRegionRequest.newBuilder().setId(regionId).build()));
    Region region = regionResponse.getRegion();
    assertEquals(firstRegion, region);
  }

  @Test
  public void getAllRegionRules() {
    // create region rules
    String rule1Id =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .addAllRegionId(List.of("region-1", "region-2"))
                            .setName("rule-1")
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                            .build())
                    .getRule()
                    .getId());

    String rule2Id =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .addAllRegionId(List.of("region-Z"))
                            .setName("rule-2")
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW)
                            .setExpirationMillis(123)
                            .build())
                    .getRule()
                    .getId());

    // get all region rules
    List<RegionRule> regionRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .getAllRegionRules(GetAllRegionRulesRequest.getDefaultInstance())
                    .getRuleList());

    assertEquals(
        List.of(
            RegionRule.newBuilder()
                .setId(rule2Id)
                .setName("rule-2")
                .addAllRegionId(List.of("region-Z"))
                .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW)
                .setExpirationMillis(123)
                .build(),
            RegionRule.newBuilder()
                .setId(rule1Id)
                .setName("rule-1")
                .addAllRegionId(List.of("region-1", "region-2"))
                .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                .build()),
        regionRules);
  }

  @Test
  public void createRegionRules() {
    // create region rules
    RegionRule regionRule1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .addAllRegionId(List.of("region-1", "region-2"))
                            .setName("rule-1")
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                            .build())
                    .getRule());

    RegionRule regionRule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .addAllRegionId(List.of("region-Z"))
                            .setName("rule-2")
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW)
                            .setExpirationMillis(123)
                            .build())
                    .getRule());

    assertEquals(
        RegionRule.newBuilder()
            .setId(regionRule1.getId())
            .setName("rule-1")
            .addAllRegionId(List.of("region-1", "region-2"))
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            .build(),
        regionRule1);

    assertEquals(
        RegionRule.newBuilder()
            .setId(regionRule2.getId())
            .setName("rule-2")
            .addAllRegionId(List.of("region-Z"))
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW)
            .setExpirationMillis(123)
            .build(),
        regionRule2);
  }

  @Test
  public void updateRegionRules() {
    // create region rules
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            regionConfigServiceStub.createRegionRule(
                CreateRegionRuleRequest.newBuilder()
                    .addAllRegionId(List.of("region-1", "region-2"))
                    .setName("rule-1")
                    .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                    .build()));

    String rule2Id =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .addAllRegionId(List.of("region-Z"))
                            .setName("rule-2")
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW)
                            .setExpirationMillis(123)
                            .build())
                    .getRule()
                    .getId());

    RegionRule regionRule =
        RegionRule.newBuilder()
            .setId(rule2Id)
            .addRegionId("region-A")
            .setName("updated-rule-2")
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            .setExpirationMillis(789)
            .build();

    // update region rule
    RegionRule updatedRegionRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .updateRegionRule(
                        UpdateRegionRuleRequest.newBuilder().setRule(regionRule).build())
                    .getRule());

    assertEquals(regionRule, updatedRegionRule);
  }

  @Test
  public void deleteRegionRules() {
    // create region rules
    String rule1Id =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .addAllRegionId(List.of("region-1", "region-2"))
                            .setName("rule-1")
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                            .build())
                    .getRule()
                    .getId());

    String rule2Id =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .addAllRegionId(List.of("region-Z"))
                            .setName("rule-2")
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW)
                            .setExpirationMillis(123)
                            .build())
                    .getRule()
                    .getId());

    // delete region rule
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            regionConfigServiceStub.deleteRegionRule(
                DeleteRegionRuleRequest.newBuilder().setId(rule2Id).build()));

    // get all region rules
    List<RegionRule> regionRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .getAllRegionRules(GetAllRegionRulesRequest.getDefaultInstance())
                    .getRuleList());

    assertEquals(
        List.of(
            RegionRule.newBuilder()
                .setId(rule1Id)
                .setName("rule-1")
                .addAllRegionId(List.of("region-1", "region-2"))
                .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                .build()),
        regionRules);
  }
}
