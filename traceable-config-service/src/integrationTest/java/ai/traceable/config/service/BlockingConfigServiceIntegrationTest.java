package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.blocking.config.service.UuidGenerator;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
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
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static RegionConfigServiceBlockingStub regionConfigServiceStub;
  private static CustomSignatureConfigServiceBlockingStub customSignatureConfigServiceStub;
  private static BlockingConfigServiceBlockingStub blockingConfigServiceStub;
  private static final UuidGenerator uuidGenerator = new UuidGenerator();

  @BeforeAll
  static void init() {
    regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    customSignatureConfigServiceStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    blockingConfigServiceStub =
        BlockingConfigServiceGrpc.newBlockingStub(managedChannelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void getBlockingRules() {
    String emptyValueUuid = uuidGenerator.generateId("");

    GetBlockingRulesResponse response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                blockingConfigServiceStub.getBlockingRules(
                    GetBlockingRulesRequest.getDefaultInstance()));

    assertEquals(emptyValueUuid, response.getRegionBlockingRules().getHash());
    assertTrue(
        response
            .getRegionBlockingRules()
            .getRegionIpBlockingRulesList()
            .isEmpty()); // rules actually empty

    assertEquals(emptyValueUuid, response.getCustomModsecBlockingRules().getHash());
    assertTrue(
        response
            .getCustomModsecBlockingRules()
            .getCustomModsecRulesBlob()
            .isEmpty()); // rules actually empty

    final String safeCrsBlockingRulesHash = response.getSafeCrsBlockingRules().getHash();
    assertNotEquals(emptyValueUuid, safeCrsBlockingRulesHash);
    assertFalse(response.getSafeCrsBlockingRules().getSafeCrsRulesBlob().isEmpty());

    int numRegions = createAndGetRegions();
    createAndGetCustomSignatureRule();
    response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                blockingConfigServiceStub.getBlockingRules(
                    GetBlockingRulesRequest.newBuilder()
                        .setRegionBlockingRulesHash(emptyValueUuid)
                        .setCustomModsecBlockingRulesHash(emptyValueUuid)
                        .setSafeCrsBlockingRulesHash(safeCrsBlockingRulesHash)
                        .build()));

    String regionBlockingRulesHash = response.getRegionBlockingRules().getHash();
    assertNotEquals(emptyValueUuid, regionBlockingRulesHash);
    assertEquals(numRegions, response.getRegionBlockingRules().getRegionIpBlockingRulesCount());

    String customModsecBlockingRulesHash = response.getCustomModsecBlockingRules().getHash();
    assertNotEquals(emptyValueUuid, customModsecBlockingRulesHash);
    assertFalse(response.getCustomModsecBlockingRules().getCustomModsecRulesBlob().isEmpty());

    assertEquals(
        safeCrsBlockingRulesHash, response.getSafeCrsBlockingRules().getHash()); // not changed
    assertTrue(response.getSafeCrsBlockingRules().getSafeCrsRulesBlob().isEmpty());

    response =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                blockingConfigServiceStub.getBlockingRules(
                    GetBlockingRulesRequest.newBuilder()
                        .setRegionBlockingRulesHash(regionBlockingRulesHash)
                        .setCustomModsecBlockingRulesHash(customModsecBlockingRulesHash)
                        .setSafeCrsBlockingRulesHash(emptyValueUuid)
                        .build()));

    assertEquals(
        regionBlockingRulesHash, response.getRegionBlockingRules().getHash()); // not changed
    assertTrue(response.getRegionBlockingRules().getRegionIpBlockingRulesList().isEmpty());

    assertEquals(
        customModsecBlockingRulesHash,
        response.getCustomModsecBlockingRules().getHash()); // not changed
    assertTrue(response.getCustomModsecBlockingRules().getCustomModsecRulesBlob().isEmpty());

    assertNotEquals(
        emptyValueUuid,
        response.getSafeCrsBlockingRules().getHash()); // we had given empty request hash
    assertFalse(response.getSafeCrsBlockingRules().getSafeCrsRulesBlob().isEmpty());
  }

  private int createAndGetRegions() {
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

    // creating region rules
    List<RegionRule> regionRules = new ArrayList<>();

    regionRules.add(
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    regionConfigServiceStub.createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .setName("rule-1")
                            .addRegionId(detailedRegions.get(0).getId())
                            .addRegionId(detailedRegions.get(1).getId())
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                            .build()))
            .getRule());

    regionRules.add(
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                regionConfigServiceStub
                    .createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .setName("rule-2")
                            .addRegionId(detailedRegions.get(2).getId())
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                            .build())
                    .getRule()));

    return detailedRegions.size();
  }

  private void createAndGetCustomSignatureRule() {
    GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                customSignatureConfigServiceStub.createCustomSignatureRule(
                    CreateCustomSignatureRuleRequest.newBuilder()
                        .setName("rule-1")
                        .setDefinition(
                            RuleDefinition.newBuilder()
                                .setClauseGroup(
                                    ClauseGroup.newBuilder()
                                        .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                                        .addClauses(
                                            Clause.newBuilder()
                                                .setMatchExpression(
                                                    MatchExpression.newBuilder()
                                                        .setMatchKey(
                                                            MatchKey.MATCH_KEY_HEADER_VALUE)
                                                        .setMatchOperator(
                                                            MatchOperator.MATCH_OPERATOR_CONTAINS)
                                                        .setMatchValue("anomalous")))))
                        .setEffect(
                            RuleEffect.newBuilder()
                                .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
                                .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
                                .build())
                        .build()))
        .getRule();
  }
}
