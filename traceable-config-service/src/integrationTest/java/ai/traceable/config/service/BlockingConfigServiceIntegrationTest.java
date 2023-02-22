package ai.traceable.config.service;

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
import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v1.GetBlockingRulesResponse;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleRequest;
import ai.traceable.customsignature.config.service.v1.CreateCustomSignatureRuleResponse;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventSeverity;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.RuleAction;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
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
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionsRequest;
import ai.traceable.region.config.service.v1.Region;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRuleActionType;
import ai.traceable.region.config.service.v1.RegionsFilter;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.type.service.client.EntityTypeServiceClient;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BlockingConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static final String TENANT_ID = "tenant-blocking-test";
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
  protected static ManagedChannel managedChannelForEntityServiceClient;
  protected static ManagedChannel managedChannelForActorServices;
  private static final List<String> actorEntityId = new ArrayList<>();
  private static final List<String> customSignatureRuleId = new ArrayList<>();

  @BeforeAll
  static void init() throws InterruptedException {
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

    detectorConfigServiceStub =
        DetectorConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    ipRangeConfigServiceStub =
        IpRangeConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    maliciousSourcesConfigServiceBlockingStub =
        MaliciousSourcesConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    managedChannelForEntityServiceClient =
        ManagedChannelBuilder.forAddress("localhost", 60061).usePlaintext().build();
    EntityTypeServiceClient entityTypeServiceClient =
        new EntityTypeServiceClient(managedChannelForEntityServiceClient);

    managedChannelForActorServices =
        ManagedChannelBuilder.forAddress("localhost", 60888).usePlaintext().build();
    actorServiceBlockingStub =
        ActorServiceGrpc.newBlockingStub(managedChannelForActorServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    // Make actor_id identifying attribute
    entityTypeServiceClient.upsertEntityType(
        TENANT_ID,
        EntityType.newBuilder()
            .setName("ACTOR")
            .setTenantId(TENANT_ID)
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName("actor_id")
                    .setIdentifyingAttribute(true)
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .build())
            .build());

    // Need to add actors upfront due to caching
    actorEntityId.add(createActor(STATUS_ALWAYS_DENIED, 0L, "", BLOCKING_CATEGORY_RATE_LIMIT));
    actorEntityId.add(
        createActor(
            STATUS_ALWAYS_ALLOWED,
            activeTimestamp,
            ENVIRONMENT_ID,
            BLOCKING_CATEGORY_THREAT_ACTOR));
    actorEntityId.add(
        createActor(
            STATUS_SNOOZED, inactiveTimestamp, ENVIRONMENT_ID, BLOCKING_CATEGORY_THREAT_ACTOR));
    actorEntityId.add(
        createActor(STATUS_SUSPENDED, activeTimestamp, "random-env", BLOCKING_CATEGORY_RATE_LIMIT));
    actorEntityId.add(
        createActor(STATUS_RESOLVED, activeTimestamp, "", BLOCKING_CATEGORY_THREAT_ACTOR));
    actorEntityId.add(
        createActor(
            STATUS_SUSPENDED,
            activeTimestamp,
            ENVIRONMENT_ID,
            BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE));
    actorEntityId.add(
        createActor(STATUS_ALWAYS_ALLOWED, 0L, "", BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE));
  }

  @AfterAll
  static void clean() {
    managedChannelForActorServices.shutdownNow();
    managedChannelForEntityServiceClient.shutdownNow();
  }

  @Test
  void getBlockingRules() {
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

    // 1 modsec rule is present + 2 * (1 threat-actors + 2 rate-limit + 2 malicious-source)
    assertNotEquals(emptyValueUuid, response.getBlockingPolicyConfiguration().getHash());
    assertEquals(11, response.getBlockingPolicyConfiguration().getBlockingDetailsListCount());

    createRegionRules();
    customSignatureRuleId.add(createCustomSignatureRule(Optional.empty()));
    customSignatureRuleId.add(createCustomSignatureRule(Optional.of(ENVIRONMENT_ID)));
    createIpTypeRule(
        "test-rule-1",
        Optional.empty(),
        List.of(
            IpLocationType.IP_LOCATION_TYPE_BOT, IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER));
    createIpTypeRule(
        "test-rule-2",
        Optional.of(ENVIRONMENT_ID),
        List.of(IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY));
    createIpTypeRule(
        "test-rule-3",
        Optional.of(ENVIRONMENT_ID),
        List.of(IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE));

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
        Set.of(IpType.IP_TYPE_BOT, IpType.IP_TYPE_HOSTING_PROVIDER),
        response.getIpTypeBlockingRules().getIpTypeRuleListList().stream()
            .map(IpTypeRule::getIpType)
            .collect(Collectors.toSet()));

    assertEquals(
        modsecCrsBlockingRulesHash, response.getSafeCrsBlockingRules().getHash()); // not changed
    assertTrue(response.getSafeCrsBlockingRules().getSafeCrsRulesBlob().isEmpty());

    // 1 modsec + 2 region + 1 custom-signature rule + 2 * (1 threat-actors + 2 rate-limit + 2
    // malicious-source) + 1 ip-type
    assertNotEquals(emptyValueUuid, response.getBlockingPolicyConfiguration().getHash());
    assertEquals(15, response.getBlockingPolicyConfiguration().getBlockingDetailsListCount());

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
        Set.of(
            IpType.IP_TYPE_PROXY,
            IpType.IP_TYPE_TOR,
            IpType.IP_TYPE_BOT,
            IpType.IP_TYPE_HOSTING_PROVIDER),
        response.getIpTypeBlockingRules().getIpTypeRuleListList().stream()
            .map(IpTypeRule::getIpType)
            .collect(Collectors.toSet()));

    String customModsecBlockingRulesHash = response.getCustomModsecBlockingRules().getHash();
    assertNotEquals(emptyValueUuid, customModsecBlockingRulesHash);
    assertFalse(response.getCustomModsecBlockingRules().getCustomModsecRulesBlob().isEmpty());

    assertEquals(
        modsecCrsBlockingRulesHash, response.getSafeCrsBlockingRules().getHash()); // not changed
    assertTrue(response.getSafeCrsBlockingRules().getSafeCrsRulesBlob().isEmpty());

    // 2 modsec + 3 region + 2 custom-signature rule + 2 * (1 threat-actors + 1 rate-limit + 2
    // malicious-source) + 3 ip-type
    String blockingPolicyConfigurationHash = response.getBlockingPolicyConfiguration().getHash();
    assertNotEquals(emptyValueUuid, response.getBlockingPolicyConfiguration().getHash());
    assertEquals(18, response.getBlockingPolicyConfiguration().getBlockingDetailsListCount());

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
    addIpRangeRule(Optional.empty(), RULE_ACTION_ALLOW);
    addIpRangeRule(Optional.of(ENVIRONMENT_ID), RULE_ACTION_BLOCK);
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
    // 2 modsec + 3 region + 2 custom-signature rule + 2 * (1 threat-actors + 1 rate-limit + 2
    // malicious-source) + 2 custom-ip + 3 ip-type
    assertEquals(20, blockingPolicyConfiguration.getBlockingDetailsListCount());

    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_IP_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(0).getCategory());
    assertEquals(
        BLOCKING_STATUS_ALLOWED, blockingPolicyConfiguration.getBlockingDetailsList(0).getStatus());
    assertEquals(
        List.of("11.11.11.11"),
        blockingPolicyConfiguration.getBlockingDetailsList(0).getIpDetails().getIpAddressesList());
    assertEquals(
        BLOCKING_CATEGORY_THREAT_ACTOR,
        blockingPolicyConfiguration.getBlockingDetailsList(1).getCategory());
    assertEquals(
        BLOCKING_STATUS_SNOOZED, blockingPolicyConfiguration.getBlockingDetailsList(1).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedThreatActorExemptionInfo(actorEntityId.get(1)),
        blockingPolicyConfiguration.getBlockingDetailsList(1).getInfo());
    assertEquals(
        BLOCKING_RULE_TYPE_ALLOW,
        blockingPolicyConfiguration.getBlockingDetailsList(1).getBlockingRuleType());
    assertEquals(
        List.of("197.23.5.0"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(1)
            .getActorDetails()
            .getIpAddressesList());
    assertEquals(
        List.of("197.23.5.0"),
        blockingPolicyConfiguration.getBlockingDetailsList(2).getIpDetails().getIpAddressesList());

    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(3).getCategory());
    assertEquals(
        BLOCKING_STATUS_ALLOWED, blockingPolicyConfiguration.getBlockingDetailsList(3).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            "Email-domain-rule-id",
            "Email-domain-rule",
            "",
            Optional.of(actorEntityId.get(6)),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.EMAIL_DOMAIN_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(3).getInfo());
    assertEquals(
        BLOCKING_RULE_TYPE_ALLOW,
        blockingPolicyConfiguration.getBlockingDetailsList(3).getBlockingRuleType());
    assertEquals(
        List.of("197.23.5.0"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(3)
            .getActorDetails()
            .getIpAddressesList());
    assertEquals(
        List.of("197.23.5.0"),
        blockingPolicyConfiguration.getBlockingDetailsList(4).getIpDetails().getIpAddressesList());

    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(5).getCategory());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            customSignatureRuleId.get(1), "rule-1", "EVENT_SEVERITY_MEDIUM"),
        blockingPolicyConfiguration.getBlockingDetailsList(5).getInfo());
    assertEquals(
        BLOCKING_STATUS_DENIED, blockingPolicyConfiguration.getBlockingDetailsList(5).getStatus());

    assertEquals(
        BLOCKING_CATEGORY_MODSECURITY,
        blockingPolicyConfiguration.getBlockingDetailsList(7).getCategory());
    assertEquals(
        BLOCKING_STATUS_DENIED, blockingPolicyConfiguration.getBlockingDetailsList(7).getStatus());
    assertEquals(
        "913100",
        blockingPolicyConfiguration.getBlockingDetailsList(7).getModsecDetails().getRuleId());
    assertEquals(
        "941280",
        blockingPolicyConfiguration.getBlockingDetailsList(8).getModsecDetails().getRuleId());

    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_IP_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(9).getCategory());
    assertEquals(
        BLOCKING_STATUS_DENIED, blockingPolicyConfiguration.getBlockingDetailsList(9).getStatus());
    assertEquals(
        List.of("11.11.11.11"),
        blockingPolicyConfiguration.getBlockingDetailsList(9).getIpDetails().getIpAddressesList());

    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(12).getCategory());
    assertEquals(
        BLOCKING_STATUS_DENIED, blockingPolicyConfiguration.getBlockingDetailsList(12).getStatus());
    assertEquals(
        List.of(IpType.IP_TYPE_TOR),
        blockingPolicyConfiguration.getBlockingDetailsList(12).getIpTypeDetails().getIpTypesList());

    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_REGION_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(15).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT,
        blockingPolicyConfiguration.getBlockingDetailsList(15).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED, blockingPolicyConfiguration.getBlockingDetailsList(15).getStatus());

    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_REGION_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(17).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(17).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED, blockingPolicyConfiguration.getBlockingDetailsList(17).getStatus());

    assertEquals(
        BLOCKING_CATEGORY_ENUMERATION,
        blockingPolicyConfiguration.getBlockingDetailsList(18).getCategory());
    assertEquals(
        BLOCKING_STATUS_DENIED, blockingPolicyConfiguration.getBlockingDetailsList(18).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedRateLimitViolationInfo(
            actorEntityId.get(0),
            "rate-limit-rule-id",
            "Rate-limit-rule",
            RateLimitCategory.RATE_LIMIT_CATEGORY_ENUMERATION),
        blockingPolicyConfiguration.getBlockingDetailsList(18).getInfo());
    assertEquals(
        List.of("197.23.5.0"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(18)
            .getActorDetails()
            .getIpAddressesList());
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

    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                regionConfigServiceStub
                    .createRegionRule(
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
                            .build())
                    .getRule());
  }

  private void createIpTypeRule(
      String name, Optional<String> environmentId, List<IpLocationType> ipLocationTypeList) {
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
                                .addConditions(
                                    MaliciousSourcesRuleCondition.newBuilder()
                                        .setIpLocationTypeCondition(
                                            IpLocationTypeCondition.newBuilder()
                                                .addAllIpLocationTypes(ipLocationTypeList))))
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
  }

  private String createCustomSignatureRule(Optional<String> environmentId) {
    CreateCustomSignatureRuleResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
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
                                                                MatchOperator
                                                                    .MATCH_OPERATOR_CONTAINS)
                                                            .setMatchValue("anomalous")))))
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
                            .setEffect(
                                RuleEffect.newBuilder()
                                    .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
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

  void addIpRangeRule(Optional<String> environmentId, RuleAction ruleAction) {
    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                ipRangeConfigServiceStub.createIpRangeRule(
                    CreateIpRangeRuleRequest.newBuilder()
                        .setRuleDetails(
                            IpRangeRuleDetails.newBuilder()
                                .setName("ip-range-rule")
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
            .setEnvironment(environment);

    if (blockingCategory == BLOCKING_CATEGORY_RATE_LIMIT) {
      actorBuilder.setStatusChangeSource(StatusChangeSource.STATUS_CHANGE_SOURCE_RATE_LIMIT);
      actorBuilder.setStatusChangeDetails(
          StatusChangeDetails.newBuilder()
              .setRateLimitDetails(
                  RateLimitDetails.newBuilder()
                      .setRuleName("Rate-limit-rule")
                      .setRuleId("rate-limit-rule-id")
                      .setRuleCategory(RateLimitCategory.RATE_LIMIT_CATEGORY_ENUMERATION)));
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
