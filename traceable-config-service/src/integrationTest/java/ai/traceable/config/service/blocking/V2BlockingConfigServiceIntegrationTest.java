package ai.traceable.config.service.blocking;

import static ai.traceable.blocking.config.service.v2.AttributeScope.ATTRIBUTE_SCOPE_REQUEST;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_ENUMERATION;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_MODSECURITY;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR;
import static ai.traceable.blocking.config.service.v2.BlockingCategory.BLOCKING_CATEGORY_TRANSACTION_BASED_DLP;
import static ai.traceable.blocking.config.service.v2.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v2.BlockingRuleType.BLOCKING_RULE_TYPE_ANALYTICS;
import static ai.traceable.blocking.config.service.v2.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v2.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;
import static ai.traceable.blocking.config.service.v2.BlockingStatus.BLOCKING_STATUS_ALLOWED;
import static ai.traceable.blocking.config.service.v2.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.blocking.config.service.v2.BlockingStatus.BLOCKING_STATUS_SNOOZED;
import static ai.traceable.blocking.config.service.v2.BlockingStatus.BLOCKING_STATUS_SUSPENDED;
import static ai.traceable.blocking.config.service.v2.IpType.IP_TYPE_BOT;
import static ai.traceable.blocking.config.service.v2.IpType.IP_TYPE_HOSTING_PROVIDER;
import static ai.traceable.blocking.config.service.v2.IpType.IP_TYPE_PROXY;
import static ai.traceable.blocking.config.service.v2.IpType.IP_TYPE_TOR;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_TESTING_DETECTION;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_QUERY;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_BODY;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_HEADER;
import static ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily.CUSTOM_RULE_FAMILY_MALICIOUS_SOURCES;
import static ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily.CUSTOM_RULE_FAMILY_SIGNATURE;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_REQUEST_HEADER;
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
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingCategory;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigServiceGrpc;
import ai.traceable.blocking.config.service.v2.BlockingConfigServiceGrpc.BlockingConfigServiceBlockingStub;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCombination;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfigurationRequest;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.CrsBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.CustomSignatureBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.CustomSignatureDetails;
import ai.traceable.blocking.config.service.v2.ExclusionRule;
import ai.traceable.blocking.config.service.v2.ExclusionRule.AnomalousAttributeCondition;
import ai.traceable.blocking.config.service.v2.ExclusionRule.EventCondition;
import ai.traceable.blocking.config.service.v2.GetBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.GetBlockingRulesResponse;
import ai.traceable.blocking.config.service.v2.InlineModification;
import ai.traceable.blocking.config.service.v2.IpType;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.IpTypeRule;
import ai.traceable.blocking.config.service.v2.RegionBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.RegionDetails;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.AgentRuleEffect;
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
import ai.traceable.customsignature.config.service.v1.FieldValue;
import ai.traceable.customsignature.config.service.v1.HeaderInjection;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.IpTypeExpression;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEffectWithModifications;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.data.classification.config.service.v1.CreateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.GlobalScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadataMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
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
import ai.traceable.platform.opa.v1.violation.DLPRuleViolationInfo;
import ai.traceable.platform.opa.v1.violation.ViolationInfoDecoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Action.Block;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition.LogicalOperator;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleResponse;
import ai.traceable.ratelimiting.config.service.v2.DataLocation;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition.EntityScope;
import ai.traceable.ratelimiting.config.service.v2.ScopeCondition.UrlScope;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
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
import com.google.protobuf.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceBlockingStub;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc.EntityTypeServiceBlockingStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V2BlockingConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static final String TENANT_ID = "tenant-blocking-test-v2";
  private static final UuidGenerator uuidGenerator = new UuidGenerator();
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final long inactiveTimestamp = System.currentTimeMillis() - 100000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 100000L;
  private static final String emptyValueUuid = uuidGenerator.generateId("");

  private static final AgentCapabilities sampleLatestAgentCapability =
      AgentCapabilities.newBuilder()
          .addComponents(Component.newBuilder().setTraceablePlatformAgentVersion("1.32.0"))
          .addComponents(Component.newBuilder().setLibtraceableVersion("0.1.98-rc.167"))
          .addComponents(Component.newBuilder().setServiceName("serviceName"))
          .build();

  private static final AgentCapabilities sampleOlderAgentCapability =
      AgentCapabilities.newBuilder()
          .addComponents(Component.newBuilder().setTraceablePlatformAgentVersion("1.30.0-rc.2"))
          .addComponents(Component.newBuilder().setLibtraceableVersion("0.1.98-rc.137"))
          .build();

  private static BlockingConfigServiceBlockingStub blockingConfigServiceStub;
  private static CustomSignatureConfigServiceBlockingStub customSignatureConfigServiceStub;
  private static DetectorConfigServiceBlockingStub detectorConfigServiceStub;
  private static IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;
  private static RegionConfigServiceBlockingStub regionConfigServiceStub;
  private static RateLimitingConfigServiceBlockingStub rateLimitingConfigStub;
  private static ActorServiceBlockingStub actorServiceBlockingStub;
  private static MaliciousSourcesConfigServiceBlockingStub
      maliciousSourcesConfigServiceBlockingStub;
  private static DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceStub;
  private static DetectionExclusionConfigServiceBlockingStub detectionExclusionConfigServiceStub;

  private static final List<String> actorEntityIds = new ArrayList<>();
  private static final List<String> customSignatureRuleIds = new ArrayList<>();
  private static final List<String> regionRuleIds = new ArrayList<>();
  private static final List<String> maliciousSourcesRuleIds = new ArrayList<>();
  private static final List<String> ipRangeRuleIds = new ArrayList<>();

  private static String serviceEntityId;
  private static String lastCreatedDLPRuleId;

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
                      RegionExpression.Region.newBuilder().setCountryIsoCode("IN").build()))
          .build();

  @BeforeEach
  void initialize() {
    actorEntityIds.clear();
    customSignatureRuleIds.clear();
    regionRuleIds.clear();
    maliciousSourcesRuleIds.clear();
    ipRangeRuleIds.clear();

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

    rateLimitingConfigStub =
        RateLimitingConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    dataClassificationConfigServiceStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    detectionExclusionConfigServiceStub =
        DetectionExclusionConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    EntityTypeServiceBlockingStub entityTypeServiceClient =
        EntityTypeServiceGrpc.newBlockingStub(
                channelRegistry.forPlaintextAddress("localhost", 60061))
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    EntityDataServiceBlockingStub entityDataServiceBlockingStub =
        EntityDataServiceGrpc.newBlockingStub(
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

    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                entityTypeServiceClient.upsertEntityType(
                    EntityType.newBuilder()
                        .setName("SERVICE")
                        .setTenantId(TENANT_ID)
                        .addAttributeType(
                            AttributeType.newBuilder()
                                .setName(
                                    EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN))
                                .setIdentifyingAttribute(true)
                                .setValueKind(AttributeKind.TYPE_STRING)
                                .build())
                        .build()));

    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType("SERVICE")
            .setEntityId("id")
            .setEntityName("serviceName")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("serviceName").build())
                    .build())
            .build();

    serviceEntityId =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> entityDataServiceBlockingStub.upsert(entity))
            .getEntityId();

    enableBlockingOnAModsecRule(Optional.empty());
    enableBlockingOnAModsecRule(Optional.of(ENVIRONMENT_ID));
    enableBlockingOnAModsecRule(Optional.empty());
  }

  @Test
  void agentVersioningModsecTest() {
    createCustomSignatureRule(
        Optional.of(ENVIRONMENT_ID), EVENT_TYPE_DETECTION_AND_BLOCKING, matchClause);

    AgentCapabilities unsetLibtraceableAgentCapability =
        AgentCapabilities.newBuilder()
            .addComponents(Component.newBuilder().setTraceablePlatformAgentVersion("1.23.2-dev.2"))
            .addComponents(Component.newBuilder().setLibtraceableVersion(""))
            .build();

    GetBlockingRulesResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setPreviousHash("")
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .addSupportedAgentCapabilities(unsetLibtraceableAgentCapability)
                                    .setCrsBlockingRulesRequest(
                                        CrsBlockingRulesRequest.getDefaultInstance()))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .addSupportedAgentCapabilities(unsetLibtraceableAgentCapability)
                                    .setCustomSignatureBlockingRulesRequest(
                                        CustomSignatureBlockingRulesRequest.getDefaultInstance()))
                            .setEnvironment(ENVIRONMENT_ID)
                            .build()));

    assertEquals(2, response.getResponseElementsCount());

    // Modsec test
    List<BlockingConfigResponseElement> modsecResponse =
        filterElements(
            response.getResponseElementsList(), BlockingConfigResponseElement::hasCrsBlockingRules);
    String modsecHash = modsecResponse.get(0).getHash();
    assertNotEquals(emptyValueUuid, modsecHash);
    assertFalse(
        modsecResponse
            .get(0)
            .getCrsBlockingRules()
            .getCrsRulesBlob()
            .contains("SecArgumentsLimit"));
    assertEquals(
        List.of(unsetLibtraceableAgentCapability),
        modsecResponse.get(0).getAgentCapabilitiesList());

    // Custom Signature
    List<BlockingConfigResponseElement> customSignatureResponse =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasCustomSignatureBlockingRules);
    String customSignatureHash = customSignatureResponse.get(0).getHash();
    assertNotEquals(emptyValueUuid, customSignatureHash);
    assertFalse(
        customSignatureResponse
            .get(0)
            .getCustomSignatureBlockingRules()
            .getCustomSignatureRulesBlob()
            .contains("SecArgumentsLimit"));
    assertEquals(
        List.of(unsetLibtraceableAgentCapability),
        customSignatureResponse.get(0).getAgentCapabilitiesList());

    assertFalse(
        customSignatureResponse
            .get(0)
            .getCustomSignatureBlockingRules()
            .getCustomSignatureRulesBlob()
            .contains("SecArgumentsLimit"));
    assertEquals(
        List.of(unsetLibtraceableAgentCapability),
        customSignatureResponse.get(0).getAgentCapabilitiesList());

    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setPreviousHash("")
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .addSupportedAgentCapabilities(sampleOlderAgentCapability)
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability)
                                    .addSupportedAgentCapabilities(unsetLibtraceableAgentCapability)
                                    .setCrsBlockingRulesRequest(
                                        CrsBlockingRulesRequest.getDefaultInstance()))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .addSupportedAgentCapabilities(sampleOlderAgentCapability)
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability)
                                    .addSupportedAgentCapabilities(unsetLibtraceableAgentCapability)
                                    .setCustomSignatureBlockingRulesRequest(
                                        CustomSignatureBlockingRulesRequest.getDefaultInstance()))
                            .setEnvironment(ENVIRONMENT_ID)
                            .build()));

    assertEquals(4, response.getResponseElementsCount());

    // Modsec
    modsecResponse =
        filterElements(
            response.getResponseElementsList(), BlockingConfigResponseElement::hasCrsBlockingRules);
    assertEquals(modsecHash, modsecResponse.get(0).getHash());
    assertFalse(modsecResponse.get(0).getCrsBlockingRules().getCrsRulesBlob().isEmpty());
    assertEquals(
        List.of(sampleOlderAgentCapability, unsetLibtraceableAgentCapability),
        modsecResponse.get(0).getAgentCapabilitiesList());

    assertNotEquals(modsecHash, modsecResponse.get(1).getHash());
    assertTrue(
        modsecResponse
            .get(1)
            .getCrsBlockingRules()
            .getCrsRulesBlob()
            .contains("SecArgumentsLimit"));
    assertEquals(
        List.of(sampleLatestAgentCapability), modsecResponse.get(1).getAgentCapabilitiesList());

    // Custom Signature
    customSignatureResponse =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasCustomSignatureBlockingRules);
    assertEquals(customSignatureHash, customSignatureResponse.get(0).getHash());
    assertFalse(
        customSignatureResponse
            .get(0)
            .getCustomSignatureBlockingRules()
            .getCustomSignatureRulesBlob()
            .isEmpty());
    assertEquals(
        List.of(sampleOlderAgentCapability, unsetLibtraceableAgentCapability),
        customSignatureResponse.get(0).getAgentCapabilitiesList());

    assertNotEquals(modsecHash, customSignatureResponse.get(1).getHash());
    assertEquals(
        List.of(sampleLatestAgentCapability),
        customSignatureResponse.get(1).getAgentCapabilitiesList());
  }

  @Test
  // This tests the same functionality as v1
  void getBlockingRulesTest() {
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

    customSignatureRuleIds.add(
        createCustomSignatureRule(
            Optional.of(ENVIRONMENT_ID), EVENT_TYPE_DETECTION_AND_BLOCKING, matchClause));

    GetBlockingRulesResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.getDefaultInstance()));
    assertEquals(emptyValueUuid, response.getHash());
    assertEquals(0, response.getResponseElementsCount());

    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setPreviousHash("")
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setBlockingPolicyConfigurationRequest(
                                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleOlderAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setRegionBlockingRulesRequest(
                                        RegionBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setCrsBlockingRulesRequest(
                                        CrsBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setCustomSignatureBlockingRulesRequest(
                                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleOlderAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setIpTypeBlockingRulesRequest(
                                        IpTypeBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .build()));

    assertNotEquals(emptyValueUuid, response.getHash());
    assertEquals(5, response.getResponseElementsCount());

    List<BlockingConfigResponseElement> filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasRegionBlockingRules);
    assertEquals(1, filteredElements.size());
    assertEquals(emptyValueUuid, filteredElements.get(0).getHash());
    assertTrue(
        filteredElements
            .get(0)
            .getRegionBlockingRules()
            .getRegionIpBlockingRulesList()
            .isEmpty()); // rules actually empty
    assertEquals(
        Collections.singletonList(sampleLatestAgentCapability),
        filteredElements.get(0).getAgentCapabilitiesList());

    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasCustomSignatureBlockingRules);
    assertEquals(1, filteredElements.size());
    assertEquals(emptyValueUuid, filteredElements.get(0).getHash());
    assertTrue(
        filteredElements
            .get(0)
            .getCustomSignatureBlockingRules()
            .getCustomSignatureRulesBlob()
            .isEmpty());
    assertEquals(
        Collections.singletonList(sampleOlderAgentCapability),
        filteredElements.get(0).getAgentCapabilitiesList());

    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasIpTypeBlockingRules);
    assertEquals(1, filteredElements.size());
    assertEquals(emptyValueUuid, filteredElements.get(0).getHash());
    assertTrue(
        filteredElements
            .get(0)
            .getIpTypeBlockingRules()
            .getIpTypeRuleListList()
            .isEmpty()); // rules actually empty
    assertEquals(
        Collections.singletonList(sampleLatestAgentCapability),
        filteredElements.get(0).getAgentCapabilitiesList());

    filteredElements =
        filterElements(
            response.getResponseElementsList(), BlockingConfigResponseElement::hasCrsBlockingRules);
    assertEquals(1, filteredElements.size());
    final String modsecCrsBlockingRulesHash = filteredElements.get(0).getHash();
    assertNotEquals(emptyValueUuid, modsecCrsBlockingRulesHash);
    assertFalse(filteredElements.get(0).getCrsBlockingRules().getCrsRulesBlob().isEmpty());
    assertEquals(
        Collections.singletonList(sampleLatestAgentCapability),
        filteredElements.get(0).getAgentCapabilitiesList());

    // 1 modsec rule is present + (1 threat-actors + 2 rate-limit + 2 malicious-source + 1 custom
    // signature rule)
    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasBlockingPolicyConfiguration);
    assertEquals(1, filteredElements.size());
    assertNotEquals(emptyValueUuid, filteredElements.get(0).getHash());
    assertEquals(
        7, filteredElements.get(0).getBlockingPolicyConfiguration().getBlockingDetailsListCount());

    // Testing that for older TAs we return IP-Details for threat-actor
    assertEquals(
        BLOCKING_CATEGORY_THREAT_ACTOR,
        filteredElements
            .get(0)
            .getBlockingPolicyConfiguration()
            .getBlockingDetailsList(0)
            .getCategory());
    assertEquals(
        List.of("197.23.5.0"),
        filteredElements
            .get(0)
            .getBlockingPolicyConfiguration()
            .getBlockingDetailsList(0)
            .getIpDetails()
            .getIpAddressesList());

    createRegionRules();

    customSignatureRuleIds.add(
        createCustomSignatureRule(Optional.empty(), EVENT_TYPE_TESTING_DETECTION, ipAddressClause));

    customSignatureRuleIds.add(
        createCustomSignatureRule(Optional.empty(), EVENT_TYPE_TESTING_DETECTION, ipTypeClause));

    customSignatureRuleIds.add(
        createCustomSignatureRule(Optional.empty(), EVENT_TYPE_TESTING_DETECTION, regionClause));

    createMaliciousSourcesRule(
        "test-rule-ipType-1",
        Optional.empty(),
        MaliciousSourcesRuleCondition.newBuilder()
            .setIpLocationTypeCondition(
                IpLocationTypeCondition.newBuilder()
                    .addAllIpLocationTypes(
                        List.of(
                            IpLocationType.IP_LOCATION_TYPE_BOT,
                            IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER))
                    .build())
            .build());

    createMaliciousSourcesRule(
        "test-rule-ipType-2",
        Optional.of(ENVIRONMENT_ID),
        MaliciousSourcesRuleCondition.newBuilder()
            .setIpLocationTypeCondition(
                IpLocationTypeCondition.newBuilder()
                    .addAllIpLocationTypes(List.of(IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY))
                    .build())
            .build());

    createMaliciousSourcesRule(
        "test-rule-ipType-3",
        Optional.of(ENVIRONMENT_ID),
        MaliciousSourcesRuleCondition.newBuilder()
            .setIpLocationTypeCondition(
                IpLocationTypeCondition.newBuilder()
                    .addAllIpLocationTypes(List.of(IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE))
                    .build())
            .build());

    createMaliciousSourcesRule(
        "test-rule-ipRange",
        Optional.of(ENVIRONMENT_ID),
        MaliciousSourcesRuleCondition.newBuilder()
            .setIpRangeCondition(
                IpAddressCondition.newBuilder()
                    .addAllIpAddresses(List.of("1.2.3.4"))
                    .addAllCidrIpRanges(List.of("1.2.3.4/32")))
            .build());

    createMaliciousSourcesRule(
        "test-rule-region",
        Optional.of(ENVIRONMENT_ID),
        MaliciousSourcesRuleCondition.newBuilder()
            .setRegionCondition(
                RegionCondition.newBuilder()
                    .addAllRegions(
                        List.of(
                            ai.traceable.malicioussources.config.service.v1.Region.newBuilder()
                                .setCountryIsoCode("AF")
                                .build())))
            .build());

    // Checking without environment
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setPreviousHash("")
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(emptyValueUuid)
                                    .setBlockingPolicyConfigurationRequest(
                                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleOlderAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(emptyValueUuid)
                                    .setRegionBlockingRulesRequest(
                                        RegionBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(modsecCrsBlockingRulesHash)
                                    .setCrsBlockingRulesRequest(
                                        CrsBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(emptyValueUuid)
                                    .setCustomSignatureBlockingRulesRequest(
                                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleOlderAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(emptyValueUuid)
                                    .setIpTypeBlockingRulesRequest(
                                        IpTypeBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .build()));

    assertNotEquals(emptyValueUuid, response.getHash());
    assertEquals(5, response.getResponseElementsCount());

    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasRegionBlockingRules);
    assertEquals(1, filteredElements.size());
    assertNotEquals(emptyValueUuid, filteredElements.get(0).getHash());
    assertEquals(
        3, filteredElements.get(0).getRegionBlockingRules().getRegionIpBlockingRulesCount());

    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasCustomSignatureBlockingRules);
    assertEquals(1, filteredElements.size());
    assertEquals(emptyValueUuid, filteredElements.get(0).getHash());
    assertTrue(
        filteredElements
            .get(0)
            .getCustomSignatureBlockingRules()
            .getCustomSignatureRulesBlob()
            .isEmpty());

    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasIpTypeBlockingRules);
    assertNotEquals(emptyValueUuid, filteredElements.get(0).getHash());
    assertEquals(
        2,
        filteredElements
            .get(0)
            .getIpTypeBlockingRules()
            .getIpTypeRuleListCount()); // rules actually empty
    assertEquals(
        Set.of(IpType.IP_TYPE_BOT, IpType.IP_TYPE_HOSTING_PROVIDER),
        filteredElements.get(0).getIpTypeBlockingRules().getIpTypeRuleListList().stream()
            .map(IpTypeRule::getIpType)
            .collect(Collectors.toSet()));

    filteredElements =
        filterElements(
            response.getResponseElementsList(), BlockingConfigResponseElement::hasCrsBlockingRules);
    assertEquals(
        modsecCrsBlockingRulesHash, filteredElements.get(0).getHash()); // hash same, not changed
    assertTrue(
        filteredElements
            .get(0)
            .getCrsBlockingRules()
            .getCrsRulesBlob()
            .isEmpty()); // But the rule is empty

    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasBlockingPolicyConfiguration);
    assertNotEquals(emptyValueUuid, filteredElements.get(0).getHash());
    assertEquals(
        13, filteredElements.get(0).getBlockingPolicyConfiguration().getBlockingDetailsListCount());

    addIpRangeRule("ip-range-rule-1", Optional.empty(), RULE_ACTION_ALLOW);
    addIpRangeRule("ip-range-rule-2", Optional.of(ENVIRONMENT_ID), RULE_ACTION_BLOCK);

    createDLPPolicy(Optional.of(ENVIRONMENT_ID));
    createExclusionRule();

    // Checking with environment
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setPreviousHash("")
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(emptyValueUuid)
                                    .setBlockingPolicyConfigurationRequest(
                                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(emptyValueUuid)
                                    .setRegionBlockingRulesRequest(
                                        RegionBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(modsecCrsBlockingRulesHash)
                                    .setCrsBlockingRulesRequest(
                                        CrsBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(emptyValueUuid)
                                    .setCustomSignatureBlockingRulesRequest(
                                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(emptyValueUuid)
                                    .setIpTypeBlockingRulesRequest(
                                        IpTypeBlockingRulesRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .setEnvironment(ENVIRONMENT_ID)
                            .build()));

    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasRegionBlockingRules);
    assertNotEquals(emptyValueUuid, filteredElements.get(0).getHash());

    assertEquals(
        4, filteredElements.get(0).getRegionBlockingRules().getRegionIpBlockingRulesCount());

    // Checking if DLP condition is included
    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasCustomSignatureBlockingRules);
    assertNotEquals(emptyValueUuid, filteredElements.get(0).getHash());

    /*
     * The following assertion seems flaky - doesn't work locally on Intellij, but works with GitHub actions
     */
    assertTrue(
        filteredElements
            .get(0)
            .getCustomSignatureBlockingRules()
            .getCustomSignatureRulesBlob()
            .contains(lastCreatedDLPRuleId));

    assertTrue(
        filteredElements
            .get(0)
            .getCustomSignatureBlockingRules()
            .getCustomSignatureRulesBlob()
            .contains(customSignatureRuleIds.get(0)));

    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasIpTypeBlockingRules);
    assertEquals(1, filteredElements.size());
    assertNotEquals(emptyValueUuid, filteredElements.get(0).getHash());
    assertEquals(
        4,
        filteredElements
            .get(0)
            .getIpTypeBlockingRules()
            .getIpTypeRuleListCount()); // rules actually empty
    assertEquals(
        Set.of(
            IpType.IP_TYPE_BOT, IpType.IP_TYPE_HOSTING_PROVIDER, IpType.IP_TYPE_PROXY, IP_TYPE_TOR),
        filteredElements.get(0).getIpTypeBlockingRules().getIpTypeRuleListList().stream()
            .map(IpTypeRule::getIpType)
            .collect(Collectors.toSet()));

    // 2 modsec + 3 region + 2 custom-signature rule + (1 threat-actors + 1 rate-limit + 2
    // malicious-sources) + 3 ip-types + 2 custom-ip + 1 DLP
    filteredElements =
        filterElements(
            response.getResponseElementsList(),
            BlockingConfigResponseElement::hasBlockingPolicyConfiguration);
    String blockingPolicyConfigurationHash = filteredElements.get(0).getHash();
    assertNotEquals(emptyValueUuid, blockingPolicyConfigurationHash);
    checkBlockingPolicy(filteredElements.get(0).getBlockingPolicyConfiguration());

    // Checking with environment, blockingPolicy hash behavior
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setPreviousHash("")
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(blockingPolicyConfigurationHash)
                                    .setBlockingPolicyConfigurationRequest(
                                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .setEnvironment(ENVIRONMENT_ID)
                            .build()));

    assertEquals(1, response.getResponseElementsList().size()); // not changed
    String globalHash = response.getHash();
    assertNotEquals(emptyValueUuid, globalHash);
    assertEquals(
        blockingPolicyConfigurationHash, response.getResponseElements(0).getHash()); // not changed
    assertTrue(
        response
            .getResponseElements(0)
            .getBlockingPolicyConfiguration()
            .getBlockingDetailsListList()
            .isEmpty());

    // Checking global hash behaviour
    response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    blockingConfigServiceStub.getBlockingRules(
                        GetBlockingRulesRequest.newBuilder()
                            .setPreviousHash(globalHash)
                            .addRequestElements(
                                BlockingConfigRequestElement.newBuilder()
                                    .setPreviousHash(blockingPolicyConfigurationHash)
                                    .setBlockingPolicyConfigurationRequest(
                                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                                    .addSupportedAgentCapabilities(sampleLatestAgentCapability))
                            .setEnvironment(ENVIRONMENT_ID)
                            .build()));
    assertEquals(
        GetBlockingRulesResponse.newBuilder()
            .setHash(globalHash)
            .setRefreshAfterDuration(Duration.newBuilder().setSeconds(30))
            .setEnabled(true)
            .build(),
        response);
  }

  void checkBlockingPolicy(BlockingPolicyConfiguration blockingPolicyConfiguration) {
    // 2 modsec + 4 region + 2 custom-signature rule + (1 threat-actors + 1 rate-limit + 4
    // malicious-source) + 2 custom-ip + 4 ip-type + 1 DLP
    assertEquals(21, blockingPolicyConfiguration.getBlockingDetailsListCount());
    assertEquals(1, blockingPolicyConfiguration.getExclusionRulesCount());
    checkExclusionPolicy(blockingPolicyConfiguration.getExclusionRules(0));

    int index = 0;
    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_ALLOW,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo(
            ipRangeRuleIds.get(0), "ip-range-rule-1"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo(
            ipRangeRuleIds.get(0), "ip-range-rule-1"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        BLOCKING_STATUS_ALLOWED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        List.of("11.11.11.11"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpDetails()
            .getIpAddressesList());

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
            .getActorDetails()
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
            .getActorDetails()
            .getIpAddressesList());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            customSignatureRuleIds.get(0),
            "rule-1",
            "EVENT_SEVERITY_MEDIUM",
            Map.of("key", "value")),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());

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
    assertEquals("913100", blockingPolicyConfiguration.getBlockingDetailsList(index).getRuleId());

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
    assertEquals("941280", blockingPolicyConfiguration.getBlockingDetailsList(index).getRuleId());

    /*
     * The following check for DLP is flaky too, since it never shows up on Intellij, but works with GitHub actions
     */
    index++;
    assertEquals(
        BLOCKING_CATEGORY_TRANSACTION_BASED_DLP,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    BlockingDetailsCombination blockingDetailsCombination =
        blockingPolicyConfiguration.getBlockingDetailsList(index).getDetailsCombination();
    assertEquals(
        BlockingDetailsCombination.ConditionsOperator.CONDITIONS_OPERATOR_AND,
        blockingDetailsCombination.getOperator());
    assertEquals(3, blockingDetailsCombination.getDetailsConditionsCount());
    assertEquals(
        BlockingDetailsCondition.newBuilder()
            .setRegionDetails(RegionDetails.newBuilder().addRegions("IN"))
            .build(),
        blockingDetailsCombination.getDetailsConditions(0));
    // URL and other conditions
    assertEquals(
        BlockingDetailsCondition.newBuilder()
            .setCustomSignatureDetails(
                CustomSignatureDetails.newBuilder().setRuleId(lastCreatedDLPRuleId))
            .build(),
        blockingDetailsCombination.getDetailsConditions(1));
    // Data type matching custom signature rules
    assertEquals(
        BlockingDetailsCombination.ConditionsOperator.CONDITIONS_OPERATOR_OR,
        blockingDetailsCombination.getDetailsConditions(2).getDetailsCombination().getOperator());
    assertEquals(
        3,
        blockingDetailsCombination
            .getDetailsConditions(2)
            .getDetailsCombination()
            .getDetailsConditionsCount());
    assertTrue(
        blockingDetailsCombination
            .getDetailsConditions(2)
            .getDetailsCombination()
            .getDetailsConditions(0)
            .hasCustomSignatureDetails());
    assertTrue(
        blockingDetailsCombination
            .getDetailsConditions(2)
            .getDetailsCombination()
            .getDetailsConditions(1)
            .hasCustomSignatureDetails());
    assertTrue(
        blockingDetailsCombination
            .getDetailsConditions(2)
            .getDetailsCombination()
            .getDetailsConditions(2)
            .hasCustomSignatureDetails());
    assertEquals(
        ViolationInfoEncoder.getEncodedDLPRuleViolationInfo(
            lastCreatedDLPRuleId, "DLP", "EVENT_SEVERITY_HIGH", Map.of("key", "value")),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    DLPRuleViolationInfo dlpRuleViolationInfo =
        ViolationInfoDecoder.getDecodedDLPRuleViolationInfo(
            blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo(), null);

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
        List.of("11.11.11.11"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpDetails()
            .getIpAddressesList());
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
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
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
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            maliciousSourcesRuleIds.get(3),
            "test-rule-ipRange",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(3),
            "test-rule-ipRange",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());

    index++;
    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_SUSPENDED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
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
            "test-rule-ipType-3",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(2),
            "test-rule-ipType-3",
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
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        List.of(IP_TYPE_PROXY),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpTypeDetails()
            .getIpTypesList());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            maliciousSourcesRuleIds.get(1),
            "test-rule-ipType-2",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(1),
            "test-rule-ipType-2",
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
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            maliciousSourcesRuleIds.get(0),
            "test-rule-ipType-1",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(0),
            "test-rule-ipType-1",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertTrue(
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getIpTypeDetails()
            .getIpTypesList()
            .containsAll(List.of(IP_TYPE_BOT, IP_TYPE_HOSTING_PROVIDER)));

    index++;
    assertEquals(
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
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
        List.of("AX"),
        blockingPolicyConfiguration
            .getBlockingDetailsList(index)
            .getRegionDetails()
            .getRegionsList());
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
        ExemptionInfoEncoder.getEncodedCustomRegionRuleExemptionInfo(
            regionRuleIds.get(2), "rule-3"),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo(
            regionRuleIds.get(2), "rule-3"),
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
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
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
        BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_BLOCK,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        ExemptionInfoEncoder.getEncodedMaliciousSourcesExemptionInfo(
            maliciousSourcesRuleIds.get(4),
            "test-rule-region",
            "EVENT_SEVERITY_HIGH",
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.REGION_CONDITION)),
        blockingPolicyConfiguration.getBlockingDetailsList(index).getInfo());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            maliciousSourcesRuleIds.get(4),
            "test-rule-region",
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

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_ANALYTICS,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    checkForRegionOrIpOrIpTypeDetails(blockingPolicyConfiguration.getBlockingDetailsList(index));

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_ANALYTICS,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    checkForRegionOrIpOrIpTypeDetails(blockingPolicyConfiguration.getBlockingDetailsList(index));

    index++;
    assertEquals(
        BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getCategory());
    assertEquals(
        BLOCKING_RULE_TYPE_ANALYTICS,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getBlockingRuleType());
    assertEquals(
        BLOCKING_STATUS_DENIED,
        blockingPolicyConfiguration.getBlockingDetailsList(index).getStatus());
    checkForRegionOrIpOrIpTypeDetails(blockingPolicyConfiguration.getBlockingDetailsList(index));
  }

  void checkForRegionOrIpOrIpTypeDetails(BlockingDetails blockingDetails) {
    if (blockingDetails.hasRegionDetails()) {
      assertEquals(List.of("IN"), blockingDetails.getRegionDetails().getRegionsList());
      assertEquals(blockingDetails.getRuleId(), customSignatureRuleIds.get(3));
    } else if (blockingDetails.hasIpDetails()) {
      assertEquals(List.of("1.2.3.4"), blockingDetails.getIpDetails().getIpAddressesList());
      assertEquals(blockingDetails.getRuleId(), customSignatureRuleIds.get(1));
    } else if (blockingDetails.hasIpTypeDetails()) {
      assertEquals(List.of(IP_TYPE_BOT), blockingDetails.getIpTypeDetails().getIpTypesList());
      assertEquals(blockingDetails.getRuleId(), customSignatureRuleIds.get(2));
    } else {
      fail(
          "BlockingDetails in this case should have either RegionDetails, IpDetails or IpTypeDetails.");
    }
    List<InlineModification> inlineModifications =
        blockingDetails.getAction().getInlineModificationsList();
    assertEquals(1, inlineModifications.size());
    ai.traceable.blocking.config.service.v2.HeaderInjection headerInjection =
        inlineModifications.get(0).getHeaderInjection();
    assertEquals(ATTRIBUTE_SCOPE_REQUEST, headerInjection.getScope());
    assertEquals("sample-header", headerInjection.getHeaderName());
    assertEquals("sample-value", headerInjection.getValue().getStaticValue());
  }

  List<BlockingConfigResponseElement> filterElements(
      List<BlockingConfigResponseElement> responseElements,
      Predicate<BlockingConfigResponseElement> predicate) {
    return responseElements.stream().filter(predicate).collect(Collectors.toUnmodifiableList());
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
                                    .addId(regions.get(2).getId()))
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
                            .addRegionId(detailedRegions.get(1).getId())
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
                            .addRegionId(detailedRegions.get(2).getId())
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

  private void createMaliciousSourcesRule(
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
    maliciousSourcesRuleIds.add(createMaliciousSourcesRuleResponse.getRule().getId());
  }

  private static String createCustomSignatureRule(
      Optional<String> environmentId, EventType eventType, Clause clause) {
    final RuleEffect ruleEffect;
    if (eventType == EVENT_TYPE_TESTING_DETECTION) {
      ruleEffect =
          RuleEffect.newBuilder()
              .setEventType(eventType)
              .setEventSeverity(EventSeverity.EVENT_SEVERITY_HIGH)
              .addEffects(
                  RuleEffectWithModifications.newBuilder()
                      .setAgentRuleEffect(
                          AgentRuleEffect.newBuilder()
                              .addAgentModifications(
                                  AgentModification.newBuilder()
                                      .setHeaderInjection(
                                          HeaderInjection.newBuilder()
                                              .setHeaderName("sample-header")
                                              .setHeaderCategory(
                                                  MatchCategory.MATCH_CATEGORY_REQUEST)
                                              .setValue(
                                                  FieldValue.newBuilder()
                                                      .setStaticValue("sample-value"))))))
              .build();
    } else {
      ruleEffect =
          RuleEffect.newBuilder()
              .setEventType(eventType)
              .setEventSeverity(EventSeverity.EVENT_SEVERITY_MEDIUM)
              .build();
    }

    CreateCustomSignatureRuleResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    customSignatureConfigServiceStub.createCustomSignatureRule(
                        CreateCustomSignatureRuleRequest.newBuilder()
                            .setName("rule-1")
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
                            .setEffect(ruleEffect)
                            .build()));

    return response.getRule().getId();
  }

  private static void createDLPPolicy(Optional<String> environmentId) {
    DataType dataType1 = createDataType("datatype-rule-1", LOCATION_REQUEST_HEADER, "^header");
    DataType dataType2 = createDataType("datatype-rule-2", LOCATION_REQUEST_BODY, "^body");
    DataType dataType3 = createDataType("datatype-rule-3", LOCATION_QUERY, "query_.*");
    DataSet dataSet = createDataSet("dataset-1", List.of(dataType1.getId(), dataType2.getId()));

    CreateRateLimitingRuleResponse response =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    rateLimitingConfigStub.createRateLimitingRule(
                        CreateRateLimitingRuleRequest.newBuilder()
                            .setData(
                                RateLimitingRuleData.newBuilder()
                                    .setCategory(Category.CATEGORY_DATA_EXFILTRATION)
                                    .setName("DLP")
                                    .putAllLabels(Map.of("key", "value"))
                                    .setEnabled(true)
                                    .setCondition(
                                        Condition.newBuilder()
                                            .setCompositeCondition(
                                                CompositeCondition.newBuilder()
                                                    .setOperator(
                                                        LogicalOperator.LOGICAL_OPERATOR_AND)
                                                    .addChildren(
                                                        Condition.newBuilder()
                                                            .setLeafCondition(
                                                                LeafCondition.newBuilder()
                                                                    .setRegionCondition(
                                                                        ai.traceable.ratelimiting
                                                                            .config.service.v2
                                                                            .RegionCondition
                                                                            .newBuilder()
                                                                            .addRegionIdentifiers(
                                                                                ai.traceable
                                                                                    .ratelimiting
                                                                                    .config.service
                                                                                    .v2
                                                                                    .RegionCondition
                                                                                    .Region
                                                                                    .newBuilder()
                                                                                    .setCountryIsoCode(
                                                                                        "IN")))))
                                                    .addChildren(
                                                        Condition.newBuilder()
                                                            .setLeafCondition(
                                                                LeafCondition.newBuilder()
                                                                    .setScopeCondition(
                                                                        ScopeCondition.newBuilder()
                                                                            .setEntityScope(
                                                                                EntityScope
                                                                                    .newBuilder()
                                                                                    .addEntityIds(
                                                                                        serviceEntityId)
                                                                                    .setEntityType(
                                                                                        ScopeCondition
                                                                                            .EntityType
                                                                                            .ENTITY_TYPE_SERVICE)))))
                                                    .addChildren(
                                                        Condition.newBuilder()
                                                            .setLeafCondition(
                                                                LeafCondition.newBuilder()
                                                                    .setDatatypeCondition(
                                                                        DatatypeCondition
                                                                            .newBuilder()
                                                                            .addDatasetIds(
                                                                                dataSet.getId())
                                                                            .addDatatypeIds(
                                                                                dataType3.getId())
                                                                            .setDataLocation(
                                                                                DataLocation
                                                                                    .DATA_LOCATION_REQUEST))))
                                                    .addChildren(
                                                        Condition.newBuilder()
                                                            .setLeafCondition(
                                                                LeafCondition.newBuilder()
                                                                    .setScopeCondition(
                                                                        ScopeCondition.newBuilder()
                                                                            .setUrlScope(
                                                                                UrlScope
                                                                                    .newBuilder()
                                                                                    .addAllUrlRegexes(
                                                                                        List.of(
                                                                                            "/order/.*",
                                                                                            "/pastOrders"))))))))
                                    .setTransactionActionConfig(
                                        TransactionActionConfig.newBuilder()
                                            .setAction(
                                                Action.newBuilder()
                                                    .setBlock(
                                                        Block.newBuilder()
                                                            .setEventSeverity(
                                                                Action.EventSeverity
                                                                    .EVENT_SEVERITY_HIGH))))
                                    .setRuleConfigScope(
                                        environmentId
                                            .map(
                                                id ->
                                                    RuleConfigScope.newBuilder()
                                                        .setEnvironmentScope(
                                                            ai.traceable.ratelimiting.config.service
                                                                .v2.EnvironmentScope.newBuilder()
                                                                .addEnvironmentIds(id))
                                                        .build())
                                            .orElse(RuleConfigScope.getDefaultInstance())))
                            .build()));
    lastCreatedDLPRuleId = response.getRule().getId();
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

  void addIpRangeRule(String ruleName, Optional<String> environmentId, RuleAction ruleAction) {
    CreateIpRangeRuleResponse createIpRangeRuleResponse =
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
    ipRangeRuleIds.add(createIpRangeRuleResponse.getRule().getId());
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
            .putAllBlockedEventLabels(Map.of("key", "value"))
            .setEnvironment(environment);

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

  private static DataSet createDataSet(String name, List<String> datatypeIds) {
    CreateDataSetRequest request =
        CreateDataSetRequest.newBuilder()
            .setInfo(
                DataSetInfo.newBuilder()
                    .setName(name)
                    .setEnabled(true)
                    .addAllDataTypeIds(datatypeIds))
            .build();

    return RequestContext.forTenantId(TENANT_ID)
        .call(() -> dataClassificationConfigServiceStub.createDataSet(request))
        .getDataSet();
  }

  private static DataType createDataType(String name, Location location, String key) {
    CreateDataTypeRequest request =
        CreateDataTypeRequest.newBuilder()
            .setRule(
                DataTypeRule.newBuilder()
                    .setName(name)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setGlobalScope(GlobalScope.newBuilder())
                            .addLocations(location)
                            .setKeyPattern(
                                StringPattern.newBuilder()
                                    .setValue(key)
                                    .setOperator(Operator.OPERATOR_MATCHES_REGEX))
                            .setAction(DataTypeRule.Action.ACTION_MATCH)))
            .build();

    return RequestContext.forTenantId(TENANT_ID)
        .call(() -> dataClassificationConfigServiceStub.createDataType(request))
        .getDataType();
  }

  private static void createExclusionRule() {
    CreateDetectionExclusionRuleRequest request =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleScope(
                DetectionExclusionRuleScope.newBuilder()
                    .setEnvironmentScope(
                        ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope
                            .newBuilder()
                            .addEnvironmentIds(
                                V2BlockingConfigServiceIntegrationTest.ENVIRONMENT_ID)))
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("detection-exclusion")
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setIpAddressCondition(
                                ai.traceable.detection.exclusion.config.service.v1
                                    .IpAddressCondition.newBuilder()
                                    .addIpAddresses("1.2.3.4")))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setAttributeMatchCondition(
                                SpanAttributeMatchCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        KeyMetadataMatchCondition.newBuilder()
                                            .setMetadata(KEY_METADATA_REQUEST_HEADER)
                                            .setMatchCondition(
                                                MatchCondition.newBuilder()
                                                    .setOperator(
                                                        ai.traceable.detection.exclusion.config
                                                            .service.v1.MatchOperator
                                                            .MATCH_OPERATOR_MATCHES_REGEX)
                                                    .setValue(
                                                        com.google.protobuf.Value.newBuilder()
                                                            .setStringValue("^apple$"))))))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setEventCondition(
                                ai.traceable.detection.exclusion.config.service.v1.EventCondition
                                    .newBuilder()
                                    .addCustomRuleEvents(
                                        CustomRuleEvent.newBuilder()
                                            .setRuleFamily(CUSTOM_RULE_FAMILY_MALICIOUS_SOURCES))
                                    .addCustomRuleEvents(
                                        CustomRuleEvent.newBuilder()
                                            .setRuleId("cs-rule")
                                            .setRuleFamily(CUSTOM_RULE_FAMILY_SIGNATURE))
                                    .addSystemDefinedEvents(
                                        SystemDefinedEvent.newBuilder()
                                            .setEventSubTypeId("crs_941280")
                                            .setEventFamily(
                                                SystemDefinedEventFamily
                                                    .SYSTEM_DEFINED_EVENT_FAMILY_MODSEC))
                                    .addSystemDefinedEvents(
                                        SystemDefinedEvent.newBuilder()
                                            .setEventTypeId("crs_931")
                                            .setEventFamily(
                                                SystemDefinedEventFamily
                                                    .SYSTEM_DEFINED_EVENT_FAMILY_MODSEC))))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setAnomalousAttributeCondition(
                                ai.traceable.detection.exclusion.config.service.v1
                                    .AnomalousAttributeCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(
                                                ai.traceable.detection.exclusion.config.service.v1
                                                    .MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setValue(
                                                com.google.protobuf.Value.newBuilder()
                                                    .setStringValue("abc")))))
                    .setRuleStatus(
                        DetectionExclusionRuleStatus.newBuilder()
                            .setDisabled(false)
                            .setRuleCreationSource(RuleSource.RULE_SOURCE_CUSTOMER))
                    .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_BLOCK))
            .build();

    RequestContext.forTenantId(TENANT_ID)
        .call(() -> detectionExclusionConfigServiceStub.createDetectionExclusionRule(request))
        .getRule()
        .getId();
  }

  void checkExclusionPolicy(ExclusionRule exclusionRule) {
    assertEquals(2, exclusionRule.getDetails().getDetailsConditionsCount());
    assertFalse(
        exclusionRule
            .getDetails()
            .getDetailsConditions(0)
            .getCustomSignatureDetails()
            .getRuleId()
            .isEmpty());
    assertEquals(
        List.of("1.2.3.4"),
        exclusionRule.getDetails().getDetailsConditions(1).getIpDetails().getIpAddressesList());
    assertEquals(3, exclusionRule.getEventConditionsCount());
    assertEquals(
        EventCondition.newBuilder()
            .addIdPrefixes("931") // Ensure crs_ is dropped
            .addIds("941280") // Ensure crs_ is dropped
            .setBlockingCategory(BLOCKING_CATEGORY_MODSECURITY)
            .build(),
        exclusionRule.getEventConditions(0));
    assertEquals(
        EventCondition.newBuilder()
            .setBlockingCategory(BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE)
            .build(),
        exclusionRule.getEventConditions(1));
    assertEquals(
        EventCondition.newBuilder()
            .addIds("cs-rule")
            .setBlockingCategory(BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE)
            .build(),
        exclusionRule.getEventConditions(2));
    assertEquals(
        List.of(
            AnomalousAttributeCondition.newBuilder()
                .setKeyExpression(
                    ai.traceable.blocking.config.service.v2.MatchExpression.newBuilder()
                        .setOperator(
                            ai.traceable.blocking.config.service.v2.MatchOperator
                                .MATCH_OPERATOR_EQUALS)
                        .setValue(com.google.protobuf.Value.newBuilder().setStringValue("abc")))
                .build()),
        exclusionRule.getAnomalousAttributeConditionsList());
    assertEquals(
        List.of(ExclusionRule.ExclusionTarget.EXCLUSION_TARGET_BLOCK),
        exclusionRule.getTargetsList());
  }
}
