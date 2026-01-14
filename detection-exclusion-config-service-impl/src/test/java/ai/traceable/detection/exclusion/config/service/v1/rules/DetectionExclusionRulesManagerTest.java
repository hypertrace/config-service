package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALERT;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_BLOCK;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionType;
import ai.traceable.detection.exclusion.config.service.v1.IpConnectionTypeCondition;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.UpsertDetectionExclusionRuleData;
import ai.traceable.detection.exclusion.config.service.v1.rules.migration.DetectionExclusionRulesMigrationManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.migration.RulesMigrationManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ExclusionModsecRulesManager;
import java.time.Clock;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionRulesManagerTest {

  private UuidGenerator uuidGenerator;
  private DetectionExclusionRulesManager rulesManager;
  private ExclusionModsecRulesManager exclusionModsecRulesManager;
  private FeatureCachingClient featureCachingClient;
  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");
  private static final DetectionExclusionRule DEFAULT_EXCLUSION_RULE =
      DetectionExclusionRule.newBuilder()
          .setId("defaultRuleId1")
          .setRuleInfo(
              DetectionExclusionRuleInfo.newBuilder()
                  .setRuleStatus(
                      DetectionExclusionRuleStatus.newBuilder()
                          .setRuleCreationSource(RuleSource.RULE_SOURCE_DEFAULT)))
          .build();

  @BeforeEach
  void setUp() {
    MockGenericConfigService mockConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any())).thenReturn(false);
    DetectionExclusionConfigServiceConfig config =
        mock(DetectionExclusionConfigServiceConfig.class);
    when(config.getDefaultDetectionExclusionRules()).thenReturn(List.of(DEFAULT_EXCLUSION_RULE));
    TimestampConverter timestampConverter = new TimestampConverter();
    DetectionExclusionRulesStore rulesStore =
        new DetectionExclusionRulesStore(
            configServiceBlockingStub,
            mockConfigChangeEventGenerator,
            featureCachingClient,
            config,
            new DetectionExclusionAuditHelper(new TimestampConverter()));
    ThresholdExceededDetectionExclusionRuleStore thresholdExceededDetectionExclusionRuleStore =
        new ThresholdExceededDetectionExclusionRuleStore(
            configServiceBlockingStub,
            mockConfigChangeEventGenerator,
            new DetectionExclusionAuditHelper(timestampConverter));
    uuidGenerator = mock(UuidGenerator.class);
    exclusionModsecRulesManager = mock(ExclusionModsecRulesManager.class);
    RulesMigrationManager rulesMigrationManager =
        mock(DetectionExclusionRulesMigrationManager.class);
    rulesManager =
        new DetectionExclusionRulesManager(
            rulesStore,
            thresholdExceededDetectionExclusionRuleStore,
            uuidGenerator,
            rulesMigrationManager,
            exclusionModsecRulesManager,
            mock(Clock.class));
  }

  @Test
  void test_bulkUpsert() {
    DetectionExclusionRuleInfo detectionExclusionRuleInfo1 =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("rule1")
            .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
            .setRuleStatus(
                DetectionExclusionRuleStatus.newBuilder()
                    .setRuleCreationSource(RuleSource.RULE_SOURCE_COUNT_THRESHOLD_EXCEEDED)
                    .build())
            .build();
    DetectionExclusionRuleInfo detectionExclusionRuleInfo2 =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("rule2")
            .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
            .setRuleStatus(DetectionExclusionRuleStatus.newBuilder().build())
            .build();

    DetectionExclusionRule detectionExclusionRule1 =
        DetectionExclusionRule.newBuilder()
            .setId("id1")
            .setRuleScope(DetectionExclusionRuleScope.getDefaultInstance())
            .setRuleInfo(detectionExclusionRuleInfo1)
            .build();

    DetectionExclusionRule detectionExclusionRule2 =
        DetectionExclusionRule.newBuilder()
            .setId("id2")
            .setRuleScope(DetectionExclusionRuleScope.getDefaultInstance())
            .setRuleInfo(detectionExclusionRuleInfo2)
            .build();

    when(uuidGenerator.generateRandomId()).thenReturn("id2");
    when(uuidGenerator.generateId("rule1" + "RULE_SOURCE_COUNT_THRESHOLD_EXCEEDED"))
        .thenReturn("id1");
    assertEquals(
        List.of(detectionExclusionRule1, detectionExclusionRule2),
        rulesManager.bulkUpsertDetectionExclusionRule(
            requestContext,
            List.of(
                UpsertDetectionExclusionRuleData.newBuilder()
                    .setUuidFromNameAndSource(true)
                    .setRuleScope(DetectionExclusionRuleScope.getDefaultInstance())
                    .setRuleInfo(detectionExclusionRuleInfo1)
                    .build(),
                UpsertDetectionExclusionRuleData.newBuilder()
                    .setRandomUuid(true)
                    .setRuleScope(DetectionExclusionRuleScope.getDefaultInstance())
                    .setRuleInfo(detectionExclusionRuleInfo2)
                    .build())));
  }

  @Test
  void testCRUDDetectionExclusionRules() {
    DetectionExclusionRuleInfo detectionExclusionRuleInfo =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("rule")
            .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
            .setRuleStatus(DetectionExclusionRuleStatus.newBuilder().build())
            .build();
    DetectionExclusionRuleScope detectionExclusionRuleScope =
        DetectionExclusionRuleScope.getDefaultInstance();
    DetectionExclusionRule detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("id")
            .setRuleInfo(detectionExclusionRuleInfo)
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    when(uuidGenerator.generateRandomId()).thenReturn("id");

    // creating detection exclusion rule
    assertEquals(
        detectionExclusionRule,
        rulesManager.createDetectionExclusionRule(
            requestContext, detectionExclusionRuleScope, detectionExclusionRuleInfo));

    // fetching detection exclusion rule
    assertTrue(
        rulesManager
            .getDetectionExclusionRules(requestContext, GetRulesFilter.getDefaultInstance())
            .contains(detectionExclusionRule));
    assertTrue(
        rulesManager
            .getDetectionExclusionRules(
                requestContext, GetRulesFilter.newBuilder().setDisabled(false).build())
            .contains(detectionExclusionRule));

    // Testing modsec fetch
    rulesManager.getDetectionExclusionModsecRules(
        requestContext,
        GetExclusionModsecRulesRequest.newBuilder()
            .setRulesFilter(GetRulesFilter.newBuilder().setDisabled(false).build())
            .addServiceNames("service-1")
            .addServiceNames("service-2")
            .build());

    verify(exclusionModsecRulesManager)
        .getModsecRules(
            requestContext,
            List.of(DEFAULT_EXCLUSION_RULE, detectionExclusionRule),
            List.of("service-1", "service-2"));

    detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("id")
            .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("rule1"))
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    // updating detection exclusion rule
    DetectionExclusionRuleInfo info =
        detectionExclusionRule.getRuleInfo().toBuilder()
            .setRuleStatus(DetectionExclusionRuleStatus.getDefaultInstance())
            .build();
    detectionExclusionRule = detectionExclusionRule.toBuilder().setRuleInfo(info).build();
    DetectionExclusionRule expectedDetectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("id")
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("rule1")
                    .addExclusionTargets(EXCLUSION_TARGET_ALERT)
                    .setRuleStatus(DetectionExclusionRuleStatus.newBuilder().build()))
            .setRuleScope(detectionExclusionRuleScope)
            .build();
    assertEquals(
        expectedDetectionExclusionRule,
        rulesManager.updateDetectionExclusionRule(requestContext, detectionExclusionRule));

    // deleting detection exclusion rule
    assertDoesNotThrow(() -> rulesManager.deleteDetectionExclusionRule(requestContext, "id"));

    // rule absent after deletion
    assertFalse(
        rulesManager
            .getDetectionExclusionRules(requestContext, GetRulesFilter.getDefaultInstance())
            .contains(expectedDetectionExclusionRule));

    // deleting default rule does not throw an exception
    assertDoesNotThrow(
        () -> rulesManager.deleteDetectionExclusionRule(requestContext, "defaultRuleId1"));
  }

  @Test
  void testProcessRawInputIpDataDetails() {
    IpAddressCondition ipAddressCondition =
        IpAddressCondition.newBuilder()
            .addAllIpAddresses(List.of("8.8.8.8"))
            .addAllCidrIpRanges(List.of("3.3.3.3/31"))
            .addAllRawInputIpData(List.of("1.2.3.4", "192.168.100.14/24", "127.0.0.1"))
            .build();
    IpAddressCondition processedIpAddressCondition =
        IpAddressCondition.newBuilder()
            .addAllRawInputIpData(List.of("1.2.3.4", "192.168.100.14/24", "127.0.0.1"))
            .addAllIpAddresses(List.of("8.8.8.8", "1.2.3.4", "127.0.0.1"))
            .addAllCidrIpRanges(List.of("3.3.3.3/31", "192.168.100.14/24"))
            .build();
    IpConnectionTypeCondition ipConnectionTypeCondition =
        IpConnectionTypeCondition.newBuilder()
            .addAllIpConnectionTypes(List.of(IpConnectionType.IP_CONNECTION_TYPE_EDUCATION))
            .build();
    DetectionExclusionRuleInfo detectionExclusionRuleInfo =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("rule")
            .addAllConditions(
                List.of(
                    DetectionExclusionCondition.newBuilder()
                        .setIpAddressCondition(ipAddressCondition)
                        .build(),
                    DetectionExclusionCondition.newBuilder()
                        .setIpConnectionTypeCondition(ipConnectionTypeCondition)
                        .build()))
            .setRuleStatus(DetectionExclusionRuleStatus.newBuilder().build())
            .build();

    DetectionExclusionRuleScope detectionExclusionRuleScope =
        DetectionExclusionRuleScope.getDefaultInstance();

    when(uuidGenerator.generateRandomId()).thenReturn("id");

    DetectionExclusionRule detectionExclusionRule =
        rulesManager.createDetectionExclusionRule(
            requestContext, detectionExclusionRuleScope, detectionExclusionRuleInfo);

    assertEquals(
        processedIpAddressCondition,
        detectionExclusionRule.getRuleInfo().getConditions(0).getIpAddressCondition());
    assertEquals(
        ipConnectionTypeCondition,
        detectionExclusionRule.getRuleInfo().getConditions(1).getIpConnectionTypeCondition());

    IpAddressCondition updatedIpAddressCondition =
        IpAddressCondition.newBuilder()
            .addAllRawInputIpData(List.of("2.3.4.5", "192.168.100.14/2", "127.0.0.2"))
            .addAllIpAddresses(List.of("1.2.3.4", "127.0.0.1"))
            .addCidrIpRanges("192.168.100.14/24")
            .build();
    IpAddressCondition processedUpdatedIpAddressCondition =
        IpAddressCondition.newBuilder()
            .addAllRawInputIpData(List.of("2.3.4.5", "192.168.100.14/2", "127.0.0.2"))
            .addAllIpAddresses(List.of("1.2.3.4", "127.0.0.1", "2.3.4.5", "127.0.0.2"))
            .addAllCidrIpRanges(List.of("192.168.100.14/24", "192.168.100.14/2"))
            .build();
    DetectionExclusionRuleInfo info =
        detectionExclusionRule.getRuleInfo().toBuilder()
            .setRuleStatus(DetectionExclusionRuleStatus.getDefaultInstance())
            .addAllConditions(
                List.of(
                    DetectionExclusionCondition.newBuilder()
                        .setIpAddressCondition(updatedIpAddressCondition)
                        .build()))
            .build();
    detectionExclusionRule = detectionExclusionRule.toBuilder().setRuleInfo(info).build();
    detectionExclusionRule =
        rulesManager.updateDetectionExclusionRule(requestContext, detectionExclusionRule);

    assertEquals(
        processedUpdatedIpAddressCondition,
        detectionExclusionRule.getRuleInfo().getConditions(2).getIpAddressCondition());
  }

  @Test
  void testMergedStatusOnUpdateDetectionExclusionRule() {
    DetectionExclusionRuleInfo detectionExclusionRuleInfo =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("rule-1")
            .addAllConditions(List.of(DetectionExclusionCondition.getDefaultInstance()))
            .setRuleStatus(
                DetectionExclusionRuleStatus.newBuilder()
                    .setRuleCreationSource(RuleSource.RULE_SOURCE_CUSTOMER)
                    .setDisabled(false)
                    .setGenerateInternalEvents(true)
                    .build())
            .build();
    DetectionExclusionRuleScope detectionExclusionRuleScope =
        DetectionExclusionRuleScope.getDefaultInstance();
    when(uuidGenerator.generateRandomId()).thenReturn("id-1");
    rulesManager.createDetectionExclusionRule(
        requestContext, detectionExclusionRuleScope, detectionExclusionRuleInfo);
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setId("id-1")
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("rule-2")
                    .setRuleStatus(
                        DetectionExclusionRuleStatus.newBuilder()
                            .setDisabled(true)
                            .setRuleCreationSource(RuleSource.RULE_SOURCE_UNSPECIFIED)
                            .setHidden(false)))
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    DetectionExclusionRule updateRule =
        rulesManager.updateDetectionExclusionRule(requestContext, rule);
    DetectionExclusionRuleStatus updatedRuleStatus = updateRule.getRuleInfo().getRuleStatus();

    assertEquals("id-1", updateRule.getId());
    assertEquals("rule-2", updateRule.getRuleInfo().getName());
    assertEquals(RuleSource.RULE_SOURCE_CUSTOMER, updatedRuleStatus.getRuleCreationSource());
    assertTrue(updatedRuleStatus.getDisabled());
    assertFalse(updatedRuleStatus.getHidden());
    assertTrue(updatedRuleStatus.getGenerateInternalEvents());

    // enabling the rule again
    rule =
        DetectionExclusionRule.newBuilder()
            .setId("id-1")
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("rule-2")
                    .setRuleStatus(DetectionExclusionRuleStatus.newBuilder().setDisabled(false)))
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    updateRule = rulesManager.updateDetectionExclusionRule(requestContext, rule);
    updatedRuleStatus = updateRule.getRuleInfo().getRuleStatus();
    assertFalse(updatedRuleStatus.getDisabled());
  }
}
