package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.DeleteRegionRuleRequest;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.ExpirationDetails;
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
import ai.traceable.region.config.service.v1.RegionRuleConditions;
import ai.traceable.region.config.service.v1.RuleScope;
import ai.traceable.region.config.service.v1.UpdateRegionRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RegionConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static RegionConfigServiceBlockingStub regionConfigServiceStub;
  private static final RuleScope ruleScope1 =
      RuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env1").build())
          .build();
  private static final RuleScope ruleScope2 =
      RuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env2").build())
          .build();

  @BeforeAll
  static void init() {
    regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(channelForInternalServices)
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
    assertEquals(245, regions.size());
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
                            .setRuleScope(ruleScope1)
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
                            .setExpirationDetails(
                                ExpirationDetails.newBuilder().setDuration("P1D").build())
                            .setRuleScope(ruleScope2)
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

    assertEquals(regionRules.size(), 2);
    RegionRule regionRule1, regionRule2;
    regionRule1 = regionRules.get(1);
    regionRule2 = regionRules.get(0);

    assertEquals(regionRule1.getId(), rule1Id);
    assertEquals(regionRule1.getName(), "rule-1");
    assertEquals(regionRule1.getRegionIdList(), List.of("region-1", "region-2"));
    assertEquals(regionRule1.getActionType(), RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK);
    assertEquals(
        List.of("env1"), regionRule1.getRuleScope().getEnvironmentScope().getEnvironmentIdsList());
    assertEquals(regionRule1.getExpirationDetails().getTimestampMillis() >= 0, true);

    assertEquals(regionRule2.getId(), rule2Id);
    assertEquals(regionRule2.getName(), "rule-2");
    assertEquals(regionRule2.getRegionIdList(), List.of("region-Z"));
    assertEquals(regionRule2.getActionType(), RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW);
    assertEquals(
        List.of("env2"), regionRule2.getRuleScope().getEnvironmentScope().getEnvironmentIdsList());
    assertEquals(regionRule2.getExpirationDetails().getTimestampMillis() > 0, true);
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
                            .setRuleScope(ruleScope1)
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
                            .setExpirationDetails(
                                ExpirationDetails.newBuilder().setDuration("P2D").build())
                            .setRuleScope(ruleScope2)
                            .build())
                    .getRule());

    assertEquals(regionRule1.getName(), "rule-1");
    assertEquals(regionRule1.getRegionIdList(), List.of("region-1", "region-2"));
    assertEquals(regionRule1.getActionType(), RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK);
    assertEquals(
        List.of("env1"), regionRule1.getRuleScope().getEnvironmentScope().getEnvironmentIdsList());
    assertEquals(regionRule1.getExpirationDetails().getTimestampMillis() >= 0, true);

    assertEquals(regionRule2.getName(), "rule-2");
    assertEquals(regionRule2.getRegionIdList(), List.of("region-Z"));
    assertEquals(regionRule2.getActionType(), RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW);
    assertEquals(
        List.of("env2"), regionRule2.getRuleScope().getEnvironmentScope().getEnvironmentIdsList());
    assertEquals(regionRule2.getExpirationDetails().getTimestampMillis() > 0, true);
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
                            .setExpirationDetails(
                                ExpirationDetails.newBuilder().setDuration("P1D").build())
                            .setRuleScope(ruleScope1)
                            .build())
                    .getRule()
                    .getId());

    RegionRule regionRule =
        RegionRule.newBuilder()
            .setId(rule2Id)
            .addRegionId("region-A")
            .setName("updated-rule-2")
            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
            .setExpirationDetails(
                RegionRule.ExpirationDetails.newBuilder()
                    .setDuration("P1D")
                    .setTimestampMillis(789)
                    .build())
            .setRuleScope(ruleScope2)
            .build();

    // update region rule
    RegionRule updatedRegionRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .updateRegionRule(
                        UpdateRegionRuleRequest.newBuilder()
                            .setId(rule2Id)
                            .addRegionId("region-A")
                            .setName("updated-rule-2")
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                            .setExpirationDetails(
                                ExpirationDetails.newBuilder().setDuration("P1D").build())
                            .setRuleScope(ruleScope2)
                            .build())
                    .getRule());

    assertEquals(regionRule.getId(), updatedRegionRule.getId());
    assertEquals(regionRule.getName(), updatedRegionRule.getName());
    assertEquals(regionRule.getRegionIdList(), updatedRegionRule.getRegionIdList());
    assertEquals(regionRule.getActionType(), updatedRegionRule.getActionType());
    assertEquals(
        regionRule.getRuleScope().getEnvironmentScope().getEnvironmentIdsList(),
        updatedRegionRule.getRuleScope().getEnvironmentScope().getEnvironmentIdsList());
    assertEquals(
        regionRule.getExpirationDetails().getDuration(),
        updatedRegionRule.getExpirationDetails().getDuration());
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
                .setRuleScope(RuleScope.getDefaultInstance())
                .setConditions(RegionRuleConditions.getDefaultInstance())
                .build()),
        regionRules);
  }
}
