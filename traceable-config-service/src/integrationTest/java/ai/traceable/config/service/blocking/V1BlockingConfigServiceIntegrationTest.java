package ai.traceable.config.service.blocking;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_IP_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_ENUMERATION;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_MODSECURITY;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_ALLOWED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_SNOOZED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_SUSPENDED;
import static ai.traceable.blocking.config.service.v1.IpType.IP_TYPE_BOT;
import static ai.traceable.blocking.config.service.v1.IpType.IP_TYPE_HOSTING_PROVIDER;
import static ai.traceable.blocking.config.service.v1.IpType.IP_TYPE_PROXY;
import static ai.traceable.blocking.config.service.v1.IpType.IP_TYPE_TOR;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_ALLOW;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_ALLOWED;
import static ai.traceable.platform.actor.v1.Status.STATUS_ALWAYS_DENIED;
import static ai.traceable.platform.actor.v1.Status.STATUS_RESOLVED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SNOOZED;
import static ai.traceable.platform.actor.v1.Status.STATUS_SUSPENDED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import ai.traceable.blocking.config.service.v1.BlockingCategory;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc;
import ai.traceable.blocking.config.service.v1.BlockingConfigServiceGrpc.BlockingConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.customsignature.config.service.v1.Category;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.IpTypeExpression;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.customsignature.config.service.v1.RuleSource;
import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleResponse;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.RuleAction;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleResponse;
import ai.traceable.malicioussources.config.service.v1.IpAddressCondition;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.RegionCondition;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.platform.actor.v1.Actor;
import ai.traceable.platform.actor.v1.Actor.Builder;
import ai.traceable.platform.actor.v1.ActorServiceGrpc;
import ai.traceable.platform.actor.v1.ActorServiceGrpc.ActorServiceBlockingStub;
import ai.traceable.platform.actor.v1.IpMetadata;
import ai.traceable.platform.actor.v1.MaliciousSourcesDetails;
import ai.traceable.platform.actor.v1.RateLimitCategory;
import ai.traceable.platform.actor.v1.RateLimitDetails;
import ai.traceable.platform.actor.v1.ScoreCategory;
import ai.traceable.platform.actor.v1.Status;
import ai.traceable.platform.actor.v1.StatusChangeDetails;
import ai.traceable.platform.actor.v1.StatusChangeSource;
import ai.traceable.platform.actor.v1.UpsertActorRequest;
import ai.traceable.platform.actor.v1.UpsertActorResponse;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.CreateRegionRuleResponse;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionsRequest;
import ai.traceable.region.config.service.v1.Region;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.RegionsFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc.EntityTypeServiceBlockingStub;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class V1BlockingConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static final String TENANT_ID = "tenant-blocking-test-v1";
  private static final UuidGenerator uuidGenerator = new UuidGenerator();
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final long inactiveTimestamp = System.currentTimeMillis() - 100000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 100000L;

  private static BlockingConfigServiceBlockingStub blockingConfigServiceStub;
  private static CustomSignatureConfigServiceBlockingStub customSignatureConfigServiceStub;
  private static DetectorConfigServiceBlockingStub detectorConfigServiceStub;
  private static IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;
  private static RegionConfigServiceBlockingStub regionConfigServiceStub;
  private static ActorServiceBlockingStub actorServiceBlockingStub;
  private static MaliciousSourcesConfigServiceBlockingStub
      maliciousSourcesConfigServiceBlockingStub;

  private static final List<String> actorEntityIds = new ArrayList<>();
  private static final List<String> customSignatureRuleIds = new ArrayList<>();
  private static final List<String> ipRangeRuleIds = new ArrayList<>();
  private static final List<String> maliciousSourcesRuleIds = new ArrayList<>();
  private static final List<String> regionRuleIds = new ArrayList<>();

  private static final Clause matchClause =
      Clause.newBuilder()
          .setMatchExpression(
              MatchExpression.newBuilder()
                  .setMatchKey(MatchKey.MATCH_KEY_HEADER_VALUE)
                  .setMatchOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                  .setMatchValue("anomalous"))
          .build();

  private static final Clause ipAddressClause =
      Clause.newBuilder()
          .setIpAddressExpression(IpAddressExpression.newBuilder().addIpAddresses("1.2.3.4"))
          .build();

  private static final Clause ipTypeClause =
      Clause.newBuilder()
          .setIpTypeExpression(
              IpTypeExpression.newBuilder()
                  .addIpTypes(ai.traceable.customsignature.config.service.v1.IpType.IP_TYPE_BOT))
          .build();

  private static final Clause regionClause =
      Clause.newBuilder()
          .setRegionExpression(
              RegionExpression.newBuilder()
                  .addRegionIdentifiers(
                      RegionExpression.Region.newBuilder().setCountryIsoCode("IN")))
          .build();

  @BeforeAll
  static void init() {
    regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    customSignatureConfigServiceStub =
        CustomSignatureConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    blockingConfigServiceStub =
        BlockingConfigServiceGrpc.newBlockingStub(channelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    detectorConfigServiceStub =
        DetectorConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    ipRangeConfigServiceStub =
        IpRangeConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    maliciousSourcesConfigServiceBlockingStub =
        MaliciousSourcesConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    EntityTypeServiceBlockingStub entityTypeServiceClient =
        EntityTypeServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress("localhost", 60061))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    actorServiceBlockingStub =
        ActorServiceGrpc.newBlockingStub(channelRegistry.forPlaintextAddress("localhost", 60888))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    // Make actor_id identifying attribute
    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                entityTypeServiceClient.upsertEntityType(
                    EntityType.newBuilder()
                        .setName("ACTOR")
                        .setTenantId(TENANT_ID)
                        .addAttributeType(
                            AttributeType.newBuilder()
                                .setName("actor_id")
                                .setIdentifyingAttribute(true)
                                .setValueKind(AttributeKind.TYPE_STRING)
                                .build())
                        .build()));

    // Need to add actors upfront due to caching
    actorEntityIds.add(createActor(STATUS_ALWAYS_DENIED, 0L, "", BLOCKING_CATEGORY_RATE_LIMIT));
    actorEntityIds.add(
        createActor(
            STATUS_ALWAYS_ALLOWED,
            activeTimestamp,
            ENVIRONMENT_ID,
            BLOCKING_CATEGORY_THREAT_ACTOR));
    actorEntityIds.add(
        createActor(
            STATUS_SNOOZED, inactiveTimestamp, ENVIRONMENT_ID, BLOCKING_CATEGORY_THREAT_ACTOR));
    actorEntityIds.add(
        createActor(STATUS_SUSPENDED, activeTimestamp, "random-env", BLOCKING_CATEGORY_RATE_LIMIT));
    actorEntityIds.add(
        createActor(STATUS_RESOLVED, activeTimestamp, "", BLOCKING_CATEGORY_THREAT_ACTOR));
    actorEntityIds.add(
        createActor(
            STATUS_SUSPENDED,
            activeTimestamp,
            ENVIRONMENT_ID,
            BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE));
    actorEntityIds.add(
        createActor(STATUS_ALWAYS_ALLOWED, 0L, "", BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE));
  }

  @Test
  void testGetBlockingRules() {
    String emptyValueUuid = uuidGenerator.generateId("");

    enableBlockingOnAModsecRule(Optional.empty());
    enableBlockingOnAModsecRule(Optional.of(ENVIRONMENT_ID));

    GetBlockingRulesResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
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

    assertEquals(emptyValueUuid, response.getIpTypeBlockingRules().getHash());
    assertTrue(
        response
            .getIpTypeBlockingRules()
            .getIpTypeRuleListList()
            .isEmpty()); // rules actually empty

    final String modsecCrsBlockingRulesHash = response.getSafeCrsBlockingRules().getHash();
    assertNotEquals(emptyValueUuid, modsecCrsBlockingRulesHash);
    assertFalse(response.getSafeCrsBlockingRules().getSafeCrsRulesBlob().isEmpty());

    // 1 modsec rule is present + (1 threat-actors + 2 rate-limit + 2 malicious-source)
    assertNotEquals(emptyValueUuid, response.getBlockingPolicyConfiguration().getHash());
    assertEquals(6, response.getBlockingPolicyConfiguration().getBlockingDetailsListCount());

    createRegionRules();

    customSignatureRuleIds.add(createCustomSignatureRule(Optional.empty(), matchClause));
    customSignatureRuleIds.add(
        createCustomSignatureRule(Optional.of(ENVIRONMENT_ID), ipAddressClause));
    customSignatureRuleIds.add(
        createCustomSignatureRule(Optional.of(ENVIRONMENT_ID), ipTypeClause));
    customSignatureRuleIds.add(
        createCustomSignatureRule(Optional.of(ENVIRONMENT_ID), regionClause));

    maliciousSourcesRuleIds.add(
        createMaliciousSourcesRule(
            "test-rule-1",
            Optional.empty(),
            MaliciousSourcesRuleCondition.newBuilder()
                .setIpLocationTypeCondition(
                    IpLocationTypeCondition.newBuilder()
                        .addAllIpLocationTypes(
                            List.of(
                                IpLocationType.IP_LOCATION_TYPE_BOT,
                                IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER)))
                .build()));

    maliciousSourcesRuleIds.add(
        createMaliciousSourcesRule(
            "test-rule-2",
            Optional.of(ENVIRONMENT_ID),
            MaliciousSourcesRuleCondition.newBuilder()
                .setIpLocationTypeCondition(
                    IpLocationTypeCondition.newBuilder()
                        .addAllIpLocationTypes(
                            List.of(IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY)))
                .build()));

    maliciousSourcesRuleIds.add(
        createMaliciousSourcesRule(
            "test-rule-3",
            Optional.of(ENVIRONMENT_ID),
            MaliciousSourcesRuleCondition.newBuilder()
                .setIpLocationTypeCondition(
                    IpLocationTypeCondition.newBuilder()
                        .addAllIpLocationTypes(
                            List.of(IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE)))
                .build()));

    // Checking without environment
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setRegionBlockingRulesHash(emptyValueUuid)
                            .setCustomModsecBlockingRulesHash(emptyValueUuid)
                            .setSafeCrsBlockingRulesHash(modsecCrsBlockingRulesHash)
                            .setIpTypeBlockingRulesHash(emptyValueUuid)
                            .setBlockingPolicyConfigurationHash(emptyValueUuid)
                            .build()));

    assertNotEquals(emptyValueUuid, response.getRegionBlockingRules().getHash());
    assertEquals(3, response.getRegionBlockingRules().getRegionIpBlockingRulesCount());

    assertNotEquals(emptyValueUuid, response.getCustomModsecBlockingRules().getHash());
    assertFalse(response.getCustomModsecBlockingRules().getCustomModsecRulesBlob().isEmpty());

    assertNotEquals(emptyValueUuid, response.getIpTypeBlockingRules().getHash());
    assertEquals(2, response.getIpTypeBlockingRules().getIpTypeRuleListList().size());
    assertEquals(
        Set.of(IP_TYPE_BOT, IP_TYPE_HOSTING_PROVIDER),
        response.getIpTypeBlockingRules().getIpTypeRuleListList().stream()
            .map(IpTypeRule::getIpType)
            .collect(Collectors.toSet()));

    assertEquals(
        modsecCrsBlockingRulesHash, response.getSafeCrsBlockingRules().getHash()); // not changed
    assertTrue(response.getSafeCrsBlockingRules().getSafeCrsRulesBlob().isEmpty());

    // 1 modsec + 2 region + 1 custom-signature rule + (1 threat-actors + 2 rate-limit + 2
    // malicious-sources) + 1 ip-type
    assertNotEquals(emptyValueUuid, response.getBlockingPolicyConfiguration().getHash());
    assertEquals(10, response.getBlockingPolicyConfiguration().getBlockingDetailsListCount());

    // Checking with environment
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setRegionBlockingRulesHash(emptyValueUuid)
                            .setCustomModsecBlockingRulesHash(emptyValueUuid)
                            .setSafeCrsBlockingRulesHash(modsecCrsBlockingRulesHash)
                            .setIpTypeBlockingRulesHash(emptyValueUuid)
                            .setBlockingPolicyConfigurationHash(emptyValueUuid)
                            .setEnvironment(ENVIRONMENT_ID)
                            .build()));

    String regionBlockingRulesHash = response.getRegionBlockingRules().getHash();
    assertNotEquals(emptyValueUuid, regionBlockingRulesHash);
    assertEquals(4, response.getRegionBlockingRules().getRegionIpBlockingRulesCount());

    String ipTypeRulesHash = response.getIpTypeBlockingRules().getHash();
    assertNotEquals(emptyValueUuid, ipTypeRulesHash);
    assertEquals(4, response.getIpTypeBlockingRules().getIpTypeRuleListList().size());
    assertEquals(
        Set.of(IpType.IP_TYPE_PROXY, IpType.IP_TYPE_TOR, IP_TYPE_BOT, IP_TYPE_HOSTING_PROVIDER),
        response.getIpTypeBlockingRules().getIpTypeRuleListList().stream()
            .map(IpTypeRule::getIpType)
            .collect(Collectors.toSet()));

    String customModsecBlockingRulesHash = response.getCustomModsecBlockingRules().getHash();
    assertNotEquals(emptyValueUuid, customModsecBlockingRulesHash);
    assertFalse(response.getCustomModsecBlockingRules().getCustomModsecRulesBlob().isEmpty());

    assertNotEquals(
        modsecCrsBlockingRulesHash,
        response.getSafeCrsBlockingRules().getHash()); // Blobs should be different
    assertFalse(response.getSafeCrsBlockingRules().getSafeCrsRulesBlob().isEmpty());

    // 2 modsec + 3 region-based + 4 custom-signature rules
    // + (1 threat-actors + 1 rate-limit + 2 malicious-source) + 3 ip-types
    String blockingPolicyConfigurationHash = response.getBlockingPolicyConfiguration().getHash();
    assertNotEquals(emptyValueUuid, response.getBlockingPolicyConfiguration().getHash());
    assertEquals(16, response.getBlockingPolicyConfiguration().getBlockingDetailsListCount());
    assertEquals(
        4,
        response.getBlockingPolicyConfiguration().getBlockingDetailsListList().stream()
            .filter(BlockingDetails::hasRegionDetails)
            .count());
    assertEquals(
        4,
        response.getBlockingPolicyConfiguration().getBlockingDetailsListList().stream()
            .filter(BlockingDetails::hasIpTypeDetails)
            .count());

    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setRegionBlockingRulesHash(regionBlockingRulesHash)
                            .setCustomModsecBlockingRulesHash(customModsecBlockingRulesHash)
                            .setSafeCrsBlockingRulesHash(emptyValueUuid)
                            .setIpTypeBlockingRulesHash(ipTypeRulesHash)
                            .setBlockingPolicyConfigurationHash(blockingPolicyConfigurationHash)
                            .setEnvironment(ENVIRONMENT_ID)
                            .build()));

    assertEquals(
        regionBlockingRulesHash, response.getRegionBlockingRules().getHash()); // not changed
    assertTrue(response.getRegionBlockingRules().getRegionIpBlockingRulesList().isEmpty());

    assertEquals(ipTypeRulesHash, response.getIpTypeBlockingRules().getHash()); // not changed
    assertTrue(response.getIpTypeBlockingRules().getIpTypeRuleListList().isEmpty());

    assertEquals(
        customModsecBlockingRulesHash,
        response.getCustomModsecBlockingRules().getHash()); // not changed
    assertTrue(response.getCustomModsecBlockingRules().getCustomModsecRulesBlob().isEmpty());

    assertNotEquals(
        emptyValueUuid,
        response.getSafeCrsBlockingRules().getHash()); // we had given empty request hash
    assertFalse(response.getSafeCrsBlockingRules().getSafeCrsRulesBlob().isEmpty());

    assertEquals(
        blockingPolicyConfigurationHash,
        response.getBlockingPolicyConfiguration().getHash()); // not changed
    assertTrue(response.getBlockingPolicyConfiguration().getBlockingDetailsListList().isEmpty());

    // Check blocking policy
    ipRangeRuleIds.add(createIpRangeRule("ip-range-rule-1", Optional.empty(), RULE_ACTION_ALLOW));
    ipRangeRuleIds.add(
        createIpRangeRule("ip-range-rule-2", Optional.of(ENVIRONMENT_ID), RULE_ACTION_BLOCK));

    maliciousSourcesRuleIds.add(
        createMaliciousSourcesRule(
            "id-2",
            Optional.of(ENVIRONMENT_ID),
            MaliciousSourcesRuleCondition.newBuilder()
                .setIpRangeCondition(
                    IpAddressCondition.newBuilder()
                        .addAllIpAddresses(List.of("1.2.3.4"))
                        .addAllCidrIpRanges(List.of("1.2.3.4/32")))
                .build()));

    maliciousSourcesRuleIds.add(
        createMaliciousSourcesRule(
            "id-3",
            Optional.of(ENVIRONMENT_ID),
            MaliciousSourcesRuleCondition.newBuilder()
                .setRegionCondition(
                    RegionCondition.newBuilder()
                        .addAllRegions(
                            List.of(
                                ai.traceable.malicioussources.config.service.v1.Region.newBuilder()
                                    .setCountryIsoCode("AF")
                                    .build())))
                .build()));

    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setRegionBlockingRulesHash(regionBlockingRulesHash)
                            .setCustomModsecBlockingRulesHash(customModsecBlockingRulesHash)
                            .setSafeCrsBlockingRulesHash(modsecCrsBlockingRulesHash)
                            .setBlockingPolicyConfigurationHash(blockingPolicyConfigurationHash)
                            .setIpTypeBlockingRulesHash(ipTypeRulesHash)
                            .setEnvironment(ENVIRONMENT_ID)
                            .build()));
    checkBlockingPolicy(response.getBlockingPolicyConfiguration());
  }

  void checkBlockingPolicy(BlockingPolicyConfiguration blockingPolicyConfiguration) {
    // 3 custom ip rules (2 created through ip range rules, 1 through malicious sources)
    // + 1 threat actor rule
    // + 4 custom signature rules (1 having match clause + 1 having ip address clause + 1 having ip
    // type clause + 1 having region clause)
    // + 2 modsecurity rules
    // + 5 malicious sources rules (2 created through email domains, 3 through ip types)
    // + 4 custom region rules (3 created through region rules, 1 through malicious sources)
    // + 1 enumeration rule
    assertEquals(20, blockingPolicyConfiguration.getBlockingDetailsListCount());

    int index = 0;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_IP_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_ALLOW,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_ALLOWED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        List.of("11.11.11.11"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpDetails()
            .getIpAddressesList());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo(
            ipRangeRuleIds.get(0), "ip-range-rule-1"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
            ipRangeRuleIds.get(0), "ip-range-rule-1"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_THREAT_ACTOR,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_STATUS_SNOOZED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo(actorEntityIds.get(index)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedThreatActorViolationInfo(actorEntityIds.get(index)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        BLOCKING_RULE_TYPE_ALLOW,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        List.of("197.23.5.0"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpDetails()
            .getIpAddressesList());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_STATUS_ALLOWED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            "Email-domain-rule-id",
            "Email-domain-rule",
            "",
            Optional.of(actorEntityIds.get(6)),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.EMAIL_DOMAIN_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "Email-domain-rule-id",
            "Email-domain-rule",
            "",
            Optional.of(actorEntityIds.get(6)),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.EMAIL_DOMAIN_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        BLOCKING_RULE_TYPE_ALLOW,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        List.of("197.23.5.0"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpDetails()
            .getIpAddressesList());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    checkForCustomSignatureOrRegionOrIpOrIpTypeDetails(
        blockingPolicyConfiguration.getBlockingDetailsList(index));

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    checkForCustomSignatureOrRegionOrIpOrIpTypeDetails(
        blockingPolicyConfiguration.getBlockingDetailsList(index));

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    checkForCustomSignatureOrRegionOrIpOrIpTypeDetails(
        blockingPolicyConfiguration.getBlockingDetailsList(index));

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    checkForCustomSignatureOrRegionOrIpOrIpTypeDetails(
        blockingPolicyConfiguration.getBlockingDetailsList(index));

    index++;
    assertEquals(
        BLOCKING_CATEGORY_MODSECURITY,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedSafeCrsViolationInfo("crs_913100"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        "913100",
        blockingPolicyConfiguration.getBlockingDetailsList(index).getModsecDetails().getRuleId());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_MODSECURITY,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedSafeCrsViolationInfo("crs_941280"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        "941280",
        blockingPolicyConfiguration.getBlockingDetailsList(index).getModsecDetails().getRuleId());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_IP_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        List.of("11.11.11.11"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpDetails()
            .getIpAddressesList());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo(
            ipRangeRuleIds.get(1), "ip-range-rule-2"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
            ipRangeRuleIds.get(1), "ip-range-rule-2"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_IP_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            maliciousSourcesRuleIds.get(3),
            "id-2",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(3),
            "id-2",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        List.of("1.2.3.4"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpDetails()
            .getIpAddressesList());
    assertEquals(
        List.of("1.2.3.4/32"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getIpDetails().getIpRangesList());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            "Email-domain-rule-id",
            "Email-domain-rule",
            "",
            Optional.of(actorEntityIds.get(5)),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.EMAIL_DOMAIN_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "Email-domain-rule-id",
            "Email-domain-rule",
            "",
            Optional.of(actorEntityIds.get(5)),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.EMAIL_DOMAIN_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        List.of("197.23.5.0"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpDetails()
            .getIpAddressesList());
    assertEquals(
        BLOCKING_STATUS_SUSPENDED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        List.of(IP_TYPE_TOR),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpTypeDetails()
            .getIpTypesList());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            maliciousSourcesRuleIds.get(2),
            "test-rule-3",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(2),
            "test-rule-3",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        List.of(IP_TYPE_PROXY),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpTypeDetails()
            .getIpTypesList());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            maliciousSourcesRuleIds.get(1),
            "test-rule-2",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(1),
            "test-rule-2",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            maliciousSourcesRuleIds.get(0),
            "test-rule-1",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(0),
            "test-rule-1",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        List.of(IP_TYPE_BOT, IP_TYPE_HOSTING_PROVIDER),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpTypeDetails()
            .getIpTypesList());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_REGION_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomRegionRuleExemptionInfo(
            regionRuleIds.get(1), "rule-2"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
            regionRuleIds.get(1), "rule-2"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        List.of("AL"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getRegionDetails()
            .getRegionsList());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_REGION_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomRegionRuleExemptionInfo(
            regionRuleIds.get(2), "rule-3"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
            regionRuleIds.get(2), "rule-3"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        List.of("DZ"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getRegionDetails()
            .getRegionsList());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_REGION_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomRegionRuleExemptionInfo(
            regionRuleIds.get(0), "rule-1"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
            regionRuleIds.get(0), "rule-1"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertTrue(
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getRegionDetails()
            .getRegionsList()
            .containsAll(List.of("AX", "AF")));
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_REGION_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            maliciousSourcesRuleIds.get(4),
            "id-3",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.REGION_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(4),
            "id-3",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.REGION_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        List.of("AF"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getRegionDetails()
            .getRegionsList());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_ENUMERATION,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
            actorEntityIds.get(0),
            "rate-limit-rule-id",
            "Rate-limit-rule",
            "LOW",
            RateLimitCategory.RATE_LIMIT_CATEGORY_ENUMERATION,
            Map.of("key", "value")),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
  }

  void checkForCustomSignatureOrRegionOrIpOrIpTypeDetails(BlockingDetails blockingDetails) {
    if (blockingDetails.hasCustomSignatureDetails()) {
      assertEquals(
          customSignatureRuleIds.get(0), blockingDetails.getCustomSignatureDetails().getRuleId());
    } else if (blockingDetails.hasRegionDetails()) {
      assertEquals(List.of("IN"), blockingDetails.getRegionDetails().getRegionsList());
    } else if (blockingDetails.hasIpDetails()) {
      assertEquals(List.of("1.2.3.4"), blockingDetails.getIpDetails().getIpAddressesList());
    } else if (blockingDetails.hasIpTypeDetails()) {
      assertEquals(List.of(IP_TYPE_BOT), blockingDetails.getIpTypeDetails().getIpTypesList());
    } else {
      fail(
          "Blocking details in this case must have either of custom signature, or region, or ip, or ip type details set");
    }
  }

  private void createRegionRules() {
    List<Region> regions =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    regionConfigServiceStub
                        .getRegions(GetRegionsRequest.getDefaultInstance())
                        .getRegionList());

    List<DetailedRegion> detailedRegions =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    regionConfigServiceStub.getDetailedRegions(
                        GetDetailedRegionsRequest.newBuilder()
                            .setFilter(
                                RegionsFilter.newBuilder()
                                    .addId(regions.get(0).getId())
                                    .addId(regions.get(1).getId())
                                    .addId(regions.get(2).getId())
                                    .addId(regions.get(3).getId()))
                            .build()))
            .getRegionList();

    CreateRegionRuleResponse createRegionRuleResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    regionConfigServiceStub.createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .setName("rule-1")
                            .addRegionId(detailedRegions.get(0).getId())
                            .addRegionId(detailedRegions.get(1).getId())
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                            .build()));
    regionRuleIds.add(createRegionRuleResponse.getRule().getId());

    createRegionRuleResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    regionConfigServiceStub.createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .setName("rule-2")
                            .addRegionId(detailedRegions.get(2).getId())
                            .setActionType(
                                RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                            .build()));
    regionRuleIds.add(createRegionRuleResponse.getRule().getId());

    createRegionRuleResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    regionConfigServiceStub.createRegionRule(
                        CreateRegionRuleRequest.newBuilder()
                            .setName("rule-3")
                            .addRegionId(detailedRegions.get(3).getId())
                            .setRuleScope(
                                ai.traceable.region.config.service.v1.RuleScope.newBuilder()
                                    .setEnvironmentScope(
                                        ai.traceable.region.config.service.v1.EnvironmentScope
                                            .newBuilder()
                                            .addEnvironmentIds(ENVIRONMENT_ID)))
                            .setActionType(RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK)
                            .build()));
    regionRuleIds.add(createRegionRuleResponse.getRule().getId());
  }

  private String createMaliciousSourcesRule(
      String name,
      Optional<String> environmentId,
      MaliciousSourcesRuleCondition maliciousSourcesRuleCondition) {
    CreateMaliciousSourcesRuleResponse createMaliciousSourcesRuleResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub.createMaliciousSourcesRule(
                        CreateMaliciousSourcesRuleRequest.newBuilder()
                            .setRuleInfo(
                                MaliciousSourcesRuleInfo.newBuilder()
                                    .setName(name)
                                    .setDescription("test-desc")
                                    .setRuleAction(
                                        MaliciousSourcesRuleAction.newBuilder()
                                            .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                                            .setEventSeverity(
                                                ai.traceable.malicioussources.config.service.v1
                                                    .EventSeverity.EVENT_SEVERITY_HIGH))
                                    .addConditions(maliciousSourcesRuleCondition))
                            .setRuleScope(
                                environmentId
                                    .map(
                                        envId ->
                                            MaliciousSourcesRuleScope.newBuilder()
                                                .setEnvironmentScope(
                                                    ai.traceable.malicioussources.config.service.v1
                                                        .EnvironmentScope.newBuilder()
                                                        .addEnvironmentIds(envId))
                                                .build())
                                    .orElse(MaliciousSourcesRuleScope.getDefaultInstance()))
                            .build()));
    return createMaliciousSourcesRuleResponse.getRule().getId();
  }

  private String createCustomSignatureRule(Optional<String> environmentId, Clause clause) {
    CreateCustomSignatureRuleResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    customSignatureConfigServiceStub.createCustomSignatureRule(
                        CreateCustomSignatureRuleRequest.newBuilder()
                            .setName("rule-1")
                            .setCategory(Category.CATEGORY_CUSTOM_SIGNATURE)
                            .setDefinition(
                                RuleDefinition.newBuilder()
                                    .putAllLabels(Map.of("key", "value"))
                                    .setClauseGroup(
                                        ClauseGroup.newBuilder()
                                            .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                                            .addClauses(clause)))
                            .setRuleScope(
                                environmentId
                                    .map(
                                        id ->
                                            RuleScope.newBuilder()
                                                .setEnvironmentScope(
                                                    EnvironmentScope.newBuilder()
                                                        .addEnvironmentIds(id))
                                                .build())
                                    .orElse(RuleScope.getDefaultInstance()))
                            .setRuleSource(RuleSource.RULE_SOURCE_TRACEABLE)
                            .setEffect(
                                RuleEffect.newBuilder()
                                    .setEventType(EVENT_TYPE_DETECTION_AND_BLOCKING)
                                    .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM))
                            .build()));
    return response.getRule().getId();
  }

  private static void enableBlockingOnAModsecRule(Optional<String> environmentId) {
    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                detectorConfigServiceStub.updateScopedAnomalyDetectionConfig(
                    UpdateScopedAnomalyDetectionConfigRequest.newBuilder()
                        .setScopedAnomalyDetectionConfig(
                            ScopedAnomalyDetectionConfig.newBuilder()
                                .setConfigScope(
                                    environmentId
                                        .map(
                                            id ->
                                                AnomalyConfigScope.newBuilder()
                                                    .setEnvironmentScope(
                                                        AnomalyEnvironmentScope.newBuilder()
                                                            .setEnvironmentId(id)))
                                        .orElse(
                                            AnomalyConfigScope.newBuilder()
                                                .setCustomerScope(
                                                    AnomalyCustomerScope.getDefaultInstance())))
                                .addAnomalyDetectionConfigs(
                                    AnomalyDetectionConfig.newBuilder()
                                        .setModsecurityAnomalyDetectionConfig(
                                            ModsecurityAnomalyDetectionConfig.newBuilder()
                                                .setModsecAnomalyRule(
                                                    ModsecurityAnomalyRuleConfig.newBuilder()
                                                        .setAnomalyRuleId(
                                                            environmentId
                                                                .map(id -> "crs_941")
                                                                .orElse("crs_913"))
                                                        .addSubRuleConfigs(
                                                            AnomalySubRuleConfig.newBuilder()
                                                                .setSubRuleId(
                                                                    environmentId
                                                                        .map(id -> "crs_941280")
                                                                        .orElse("crs_913100"))
                                                                .setBlockingEnabled(true))))))
                        .build()));
  }

  private String createIpRangeRule(
      String ruleName, Optional<String> environmentId, RuleAction ruleAction) {
    CreateIpRangeRuleResponse ipRangeRuleResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    ipRangeConfigServiceStub.createIpRangeRule(
                        CreateIpRangeRuleRequest.newBuilder()
                            .setRuleDetails(
                                IpRangeRuleDetails.newBuilder()
                                    .setName(ruleName)
                                    .setRuleAction(ruleAction)
                                    .addRawInputIpData("11.11.11.11"))
                            .setRuleScope(
                                environmentId
                                    .map(
                                        id ->
                                            ai.traceable.iprange.config.service.v1.RuleScope
                                                .newBuilder()
                                                .setEnvironmentScope(
                                                    ai.traceable.iprange.config.service.v1
                                                        .EnvironmentScope.newBuilder()
                                                        .addEnvironmentIds(id))
                                                .build())
                                    .orElse(
                                        ai.traceable.iprange.config.service.v1.RuleScope
                                            .getDefaultInstance()))
                            .build()));
    return ipRangeRuleResponse.getRule().getId();
  }

  private static String createActor(
      Status status, Long expiry, String environment, BlockingCategory blockingCategory) {
    Builder actorBuilder =
        Actor.newBuilder()
            .setActorId("user-" + environment + "-" + status.name())
            .setScoreCategory(ScoreCategory.SCORE_CATEGORY_HIGH)
            .setStatus(status)
            .addIpAddresses("197.23.5.0")
            .addIpMetadata(
                IpMetadata.newBuilder()
                    .setLastActivityTimestampMillis(20)
                    .setIpTraitJson("iptrait")
                    .setIpAddress("197.23.5.0"))
            .addAllLabels(List.of("label1"))
            .setStatusExpiryTimestamp(expiry)
            .setEnvironment(environment)
            .putAllBlockedEventLabels(Map.of("key", "value"));

    if (blockingCategory == BLOCKING_CATEGORY_RATE_LIMIT) {
      actorBuilder.setStatusChangeSource(StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT);
      actorBuilder.setStatusChangeDetails(
          StatusChangeDetails.newBuilder()
              .setRateLimitDetails(
                  RateLimitDetails.newBuilder()
                      .setRuleName("Rate-limit-rule")
                      .setRuleId("rate-limit-rule-id")
                      .setRuleCategory(RateLimitCategory.RATE_LIMIT_CATEGORY_ENUMERATION)
                      .setRuleSeverity("LOW")));
    } else if (blockingCategory == BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE) {
      actorBuilder.setStatusChangeSource(StatusChangeSource.STATUS_CHANGE_SOURCE_MALICIOUS_SOURCES);
      actorBuilder.setStatusChangeDetails(
          StatusChangeDetails.newBuilder()
              .setMaliciousSourcesDetails(
                  MaliciousSourcesDetails.newBuilder()
                      .setRuleName("Email-domain-rule")
                      .setRuleId("Email-domain-rule-id")));
    }
    Actor actor = actorBuilder.build();
    UpsertActorResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    actorServiceBlockingStub.upsertActor(
                        UpsertActorRequest.newBuilder().setActor(actor).build()));
    return response.getActor().getEntityId();
  }
}
