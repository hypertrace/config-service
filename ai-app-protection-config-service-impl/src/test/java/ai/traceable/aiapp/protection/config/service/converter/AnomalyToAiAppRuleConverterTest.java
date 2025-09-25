package ai.traceable.aiapp.protection.config.service.converter;

import static ai.traceable.aiapp.protection.config.service.converter.AiAppConverterConstants.THREAT_TYPE_ID_LABEL_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import ai.traceable.aiapp.protection.config.service.v1.Action;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppCustomRuleData;
import ai.traceable.aiapp.protection.config.service.v1.AiAppOotbRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppRule;
import ai.traceable.aiapp.protection.config.service.v1.AiAppSubRule;
import ai.traceable.aiapp.protection.config.service.v1.AiInputExplosionRuleData;
import ai.traceable.aiapp.protection.config.service.v1.EnvironmentScope;
import ai.traceable.aiapp.protection.config.service.v1.EventDetails;
import ai.traceable.aiapp.protection.config.service.v1.ModelGovernanceRuleData;
import ai.traceable.aiapp.protection.config.service.v1.RuleAction;
import ai.traceable.aiapp.protection.config.service.v1.RuleScope;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatus;
import ai.traceable.aiapp.protection.config.service.v1.RuleStatusDetails;
import ai.traceable.aiapp.protection.config.service.v1.SeverityLevel;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventDetails;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySeverityLevel;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfigMap;
import ai.traceable.anomaly.config.service.v1.detector.CodeDetectedInPromptAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CodeDetectedInPromptThreatRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.GenAiAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.LlmModelGovernanceAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AnomalyToAiAppRuleConverterTest {

  private static final String CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID = "codeDetectedInPrompt";
  private AnomalyToAiAppRuleConverter anomalyToAiAppRuleConverter;
  private RequestContext mockRequestContext;

  @BeforeEach
  void setUp() {
    // Create mock FeatureCachingClient
    ai.traceable.config.service.feature.caching.client.FeatureCachingClient
        mockFeatureCachingClient =
            Mockito.mock(
                ai.traceable.config.service.feature.caching.client.FeatureCachingClient.class);

    // Create mock RequestContext
    this.mockRequestContext = Mockito.mock(RequestContext.class);

    // Configure mock to return empty set by default (no hidden rules)
    when(mockFeatureCachingClient.getHiddenDefenseAiFeatures(any(RequestContext.class)))
        .thenReturn(Set.of());

    this.anomalyToAiAppRuleConverter = new AnomalyToAiAppRuleConverter(mockFeatureCachingClient);
  }

  @Test
  void testConvertToAiAppRules_WithGenAiConfigAndCustomRules() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfos();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedAnomalyDetectionConfig();
    List<AiAppCustomRule> customRules = createTestCustomRules();
    AnomalyConfigScope configScope = createTestConfigScope();
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs = Arrays.asList();

    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(2, result.size());

    // Verify first rule (Model Governance)
    AiAppRule modelGovernanceRule =
        result.stream()
            .filter(rule -> "rule-model-governance".equals(rule.getRuleId()))
            .findFirst()
            .orElseThrow();

    assertEquals("rule-model-governance", modelGovernanceRule.getRuleId());
    assertEquals("Model Governance Rule", modelGovernanceRule.getRuleName());
    assertEquals(2, modelGovernanceRule.getEventLabelsMap().size());
    assertEquals("ai_security", modelGovernanceRule.getEventLabelsMap().get("category"));
    assertEquals("high", modelGovernanceRule.getEventLabelsMap().get("priority"));

    // Verify event details
    assertTrue(modelGovernanceRule.hasEventDetails());
    EventDetails eventDetails = modelGovernanceRule.getEventDetails();
    assertEquals("Model governance violation detected", eventDetails.getDescription());
    assertEquals("Review and update model usage policies", eventDetails.getMitigation());
    assertEquals("Potential unauthorized model usage", eventDetails.getImpact());
    assertEquals("https://docs.traceable.ai/model-governance", eventDetails.getReferences());

    // Verify rule status (should be default values since not derived from sub rule configs)
    assertTrue(modelGovernanceRule.hasRuleStatus());
    RuleStatus ruleStatus = modelGovernanceRule.getRuleStatus();
    assertFalse(ruleStatus.getDisabled());
    assertFalse(ruleStatus.getInternal());

    // Verify sub rules (both OOTB sub rules from AnomalySubRuleInfo and custom rules)
    assertEquals(2, modelGovernanceRule.getAiAppSubRulesCount()); // 1 OOTB + 1 custom

    // Find OOTB sub rule
    AiAppSubRule ootbSubRule =
        modelGovernanceRule.getAiAppSubRulesList().stream()
            .filter(AiAppSubRule::hasOotbRule)
            .findFirst()
            .orElseThrow();

    AiAppOotbRule ootbRule = ootbSubRule.getOotbRule();
    assertEquals("sub-rule-model-governance", ootbRule.getRuleId());
    assertEquals("Model Governance Sub Rule", ootbRule.getRuleName());
    assertEquals(SeverityLevel.SEVERITY_LEVEL_HIGH, ootbRule.getSeverityLevel());
    assertEquals(RuleAction.RULE_ACTION_MONITOR, ootbRule.getRuleAction());
    assertTrue(ootbRule.getInternal());

    // Find custom sub rule
    AiAppSubRule customSubRule =
        modelGovernanceRule.getAiAppSubRulesList().stream()
            .filter(AiAppSubRule::hasCustomRule)
            .findFirst()
            .orElseThrow();

    assertEquals("custom-rule-1", customSubRule.getCustomRule().getRuleId());

    // Verify second rule (Input Explosion)
    AiAppRule inputExplosionRule =
        result.stream()
            .filter(rule -> "rule-input-explosion".equals(rule.getRuleId()))
            .findFirst()
            .orElseThrow();

    assertEquals("rule-input-explosion", inputExplosionRule.getRuleId());
    assertEquals("Input Explosion Rule", inputExplosionRule.getRuleName());
    assertEquals(1, inputExplosionRule.getAiAppSubRulesCount());
    assertEquals(
        "custom-rule-2", inputExplosionRule.getAiAppSubRules(0).getCustomRule().getRuleId());
  }

  @Test
  void testConvertToAiAppRules_EmptyInputs() {
    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            createTestConfigScope(),
            null,
            Arrays.asList(),
            Arrays.asList(),
            Arrays.asList());

    // Assert
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testConvertToAiAppRules_NoMatchingCustomRules() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfos();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedAnomalyDetectionConfig();
    List<AiAppCustomRule> customRules = Arrays.asList(); // Empty custom rules
    AnomalyConfigScope configScope = createTestConfigScope();
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs = Arrays.asList();

    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(2, result.size());

    // Verify OOTB sub rules are still present, but no custom sub rules
    for (AiAppRule rule : result) {
      if ("rule-model-governance".equals(rule.getRuleId())) {
        // Model governance rule should have 1 OOTB sub rule (from AnomalySubRuleInfo)
        assertEquals(1, rule.getAiAppSubRulesCount());
        assertTrue(rule.getAiAppSubRules(0).hasOotbRule());
      } else {
        // Input explosion rule has no sub rule infos, so should have 0 sub rules
        assertEquals(0, rule.getAiAppSubRulesCount());
      }
    }
  }

  @Test
  void testConvertToAiAppRules_NoGenAiConfig() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfos();
    ScopedAnomalyDetectionConfig scopedConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(AnomalyConfigScope.newBuilder().build())
            .build(); // Empty scoped config
    List<AiAppCustomRule> customRules = createTestCustomRules();
    AnomalyConfigScope configScope = createTestConfigScope();
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs = Arrays.asList();

    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(2, result.size());

    // Verify default rule status when no GenAI config
    for (AiAppRule rule : result) {
      assertTrue(rule.hasRuleStatus());
      assertFalse(rule.getRuleStatus().getDisabled());
      assertFalse(rule.getRuleStatus().getInternal());
    }
  }

  private List<AnomalyRuleInfo> createTestAnomalyRuleInfos() {
    Map<String, String> eventLabels1 = new HashMap<>();
    eventLabels1.put("category", "ai_security");
    eventLabels1.put("priority", "high");

    AnomalyEventDetails eventDetails1 =
        AnomalyEventDetails.newBuilder()
            .setDescription("Model governance violation detected")
            .setMitigation("Review and update model usage policies")
            .setImpact("Potential unauthorized model usage")
            .setReferences("https://docs.traceable.ai/model-governance")
            .build();

    // Create sub rule info for model governance rule
    Map<String, String> subRuleEventLabels1 = new HashMap<>();
    subRuleEventLabels1.put("sub_category", "model_validation");

    AnomalyEventDetails subRuleEventDetails1 =
        AnomalyEventDetails.newBuilder()
            .setDescription("Model governance sub rule violation")
            .setMitigation("Check model permissions")
            .setImpact("Unauthorized model access")
            .setReferences("https://docs.traceable.ai/sub-rules")
            .build();

    AnomalySubRuleInfo subRuleInfo1 =
        AnomalySubRuleInfo.newBuilder()
            .setRuleId("sub-rule-model-governance")
            .setRuleName("Model Governance Sub Rule")
            .putAllEventLabels(subRuleEventLabels1)
            .setSeverityLevel(AnomalySeverityLevel.ANOMALY_SEVERITY_LEVEL_HIGH)
            .setEventDetails(subRuleEventDetails1)
            .build();

    AnomalyRuleInfo ruleInfo1 =
        AnomalyRuleInfo.newBuilder()
            .setRuleId("rule-model-governance")
            .setRuleName("Model Governance Rule")
            .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_GEN_AI)
            .putAllEventLabels(eventLabels1)
            .setEventDetails(eventDetails1)
            .addSubRuleInfos(subRuleInfo1)
            .build();

    Map<String, String> eventLabels2 = new HashMap<>();
    eventLabels2.put("category", "input_validation");

    AnomalyEventDetails eventDetails2 =
        AnomalyEventDetails.newBuilder()
            .setDescription("Large input detected")
            .setMitigation("Implement input size limits")
            .setImpact("Potential resource exhaustion")
            .setReferences("https://docs.traceable.ai/input-limits")
            .build();

    AnomalyRuleInfo ruleInfo2 =
        AnomalyRuleInfo.newBuilder()
            .setRuleId("rule-input-explosion")
            .setRuleName("Input Explosion Rule")
            .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_GEN_AI)
            .putAllEventLabels(eventLabels2)
            .setEventDetails(eventDetails2)
            .build();

    return Arrays.asList(ruleInfo1, ruleInfo2);
  }

  private ScopedAnomalyDetectionConfig createTestScopedAnomalyDetectionConfig() {
    // Create sub rule config with internal flag - matching the sub rule ID from AnomalySubRuleInfo
    AnomalySubRuleConfig subRuleConfig =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("sub-rule-model-governance")
            .setInternal(true)
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)
            .build();

    Map<String, AnomalySubRuleConfig> subRuleConfigMap = new HashMap<>();
    subRuleConfigMap.put("sub-rule-model-governance", subRuleConfig);

    AnomalySubRuleConfigMap subRuleConfigMapWrapper =
        AnomalySubRuleConfigMap.newBuilder().putAllSubRuleConfigs(subRuleConfigMap).build();

    GenAiAnomalyDetectionConfig genAiConfig1 =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("rule-model-governance")
            .setLlmModelGovernance(LlmModelGovernanceAnomalyDetectionConfig.newBuilder().build())
            .setSubRuleConfigs(subRuleConfigMapWrapper)
            .build();

    GenAiAnomalyDetectionConfig genAiConfig2 =
        GenAiAnomalyDetectionConfig.newBuilder().setAnomalyRuleId("rule-input-explosion").build();

    AnomalyDetectionConfig detectionConfig1 =
        AnomalyDetectionConfig.newBuilder().setGenAiAnomalyDetectionConfig(genAiConfig1).build();

    AnomalyDetectionConfig detectionConfig2 =
        AnomalyDetectionConfig.newBuilder().setGenAiAnomalyDetectionConfig(genAiConfig2).build();

    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(AnomalyConfigScope.newBuilder().build())
        .addAnomalyDetectionConfigs(detectionConfig1)
        .addAnomalyDetectionConfigs(detectionConfig2)
        .build();
  }

  private AnomalyConfigScope createTestConfigScope() {
    return AnomalyConfigScope.newBuilder()
        .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
        .build();
  }

  private List<AiAppCustomRule> createTestCustomRules() {
    // Custom rule 1 - matches model governance rule
    Map<String, String> eventLabels1 = new HashMap<>();
    eventLabels1.put(THREAT_TYPE_ID_LABEL_KEY, "rule-model-governance");
    eventLabels1.put("custom_label", "model_check");

    AiAppCustomRuleData ruleData1 =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Custom Model Governance Rule")
            .setDescription("Custom rule for model governance")
            .setEnabled(true)
            .setRuleStatusDetails(
                RuleStatusDetails.newBuilder()
                    .setHidden(false)
                    .setInternal(false)
                    .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_CUSTOMER)
                    .build())
            .putAllEventLabels(eventLabels1)
            .setModelGovernanceRuleData(ModelGovernanceRuleData.newBuilder().build())
            .setAction(
                Action.newBuilder()
                    .setAlert(
                        Action.Alert.newBuilder()
                            .setSeverityLevel(SeverityLevel.SEVERITY_LEVEL_HIGH)
                            .build())
                    .build())
            .build();

    AiAppCustomRule customRule1 =
        AiAppCustomRule.newBuilder().setRuleId("custom-rule-1").setRuleData(ruleData1).build();

    // Custom rule 2 - matches input explosion rule
    Map<String, String> eventLabels2 = new HashMap<>();
    eventLabels2.put(THREAT_TYPE_ID_LABEL_KEY, "rule-input-explosion");
    eventLabels2.put("custom_label", "input_size_check");

    AiAppCustomRuleData ruleData2 =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Custom Input Explosion Rule")
            .setDescription("Custom rule for input explosion detection")
            .setEnabled(true)
            .setRuleStatusDetails(
                RuleStatusDetails.newBuilder()
                    .setHidden(false)
                    .setInternal(true)
                    .setRuleCreationSource(RuleStatusDetails.RuleSource.RULE_SOURCE_TRACEABLE)
                    .build())
            .putAllEventLabels(eventLabels2)
            .setAiInputExplosionRuleData(
                AiInputExplosionRuleData.newBuilder().setInputCharacterLimit(10000).build())
            .setAction(
                Action.newBuilder()
                    .setMarkForTesting(Action.MarkForTesting.newBuilder().build())
                    .build())
            .build();

    AiAppCustomRule customRule2 =
        AiAppCustomRule.newBuilder().setRuleId("custom-rule-2").setRuleData(ruleData2).build();

    // Custom rule 3 - no matching threat type ID (should not be included)
    Map<String, String> eventLabels3 = new HashMap<>();
    eventLabels3.put(THREAT_TYPE_ID_LABEL_KEY, "non-matching-rule-id");

    AiAppCustomRuleData ruleData3 =
        AiAppCustomRuleData.newBuilder()
            .setRuleName("Non-matching Custom Rule")
            .setDescription("This rule should not match any anomaly rule")
            .setEnabled(true)
            .putAllEventLabels(eventLabels3)
            .build();

    AiAppCustomRule customRule3 =
        AiAppCustomRule.newBuilder().setRuleId("custom-rule-3").setRuleData(ruleData3).build();

    return Arrays.asList(customRule1, customRule2, customRule3);
  }

  @Test
  void testConvertToAiAppRules_WithOverriddenDefaultAndOverridingChildScopes() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfos();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedAnomalyDetectionConfig();
    List<AiAppCustomRule> customRules = Arrays.asList();
    AnomalyConfigScope configScope = createTestConfigScope();

    // Create unresolved configs with environment-level overrides
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs = createUnresolvedConfigsWithOverrides();

    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(2, result.size());

    // Verify first rule (Model Governance) has overridden default and overriding child scopes
    AiAppRule modelGovernanceRule =
        result.stream()
            .filter(rule -> "rule-model-governance".equals(rule.getRuleId()))
            .findFirst()
            .orElseThrow();

    // Verify main rule override information
    assertTrue(modelGovernanceRule.getOverriddenDefault(), "Main rule should be overridden");
    assertEquals(
        2,
        modelGovernanceRule.getOverridingChildScopesCount(),
        "Should have 2 overriding child scopes");

    // Verify overriding child scopes contain environment scopes
    boolean foundEnv1 = false;
    boolean foundEnv2 = false;
    for (RuleScope scope : modelGovernanceRule.getOverridingChildScopesList()) {
      if (scope.hasEnvironmentScope()) {
        EnvironmentScope envScope = scope.getEnvironmentScope();
        if (envScope.getEnvironmentIdsList().contains("env-123")) {
          foundEnv1 = true;
        }
        if (envScope.getEnvironmentIdsList().contains("env-456")) {
          foundEnv2 = true;
        }
      }
    }
    assertTrue(foundEnv1, "Should have env-123 in overriding child scopes");
    assertTrue(foundEnv2, "Should have env-456 in overriding child scopes");

    // Verify sub-rule override information
    assertEquals(1, modelGovernanceRule.getAiAppSubRulesCount());
    AiAppSubRule subRule = modelGovernanceRule.getAiAppSubRules(0);
    assertTrue(subRule.hasOotbRule());
    AiAppOotbRule ootbRule = subRule.getOotbRule();

    assertTrue(ootbRule.getOverriddenDefault(), "Sub-rule should be overridden");
    assertEquals(
        1,
        ootbRule.getOverridingChildScopesCount(),
        "Sub-rule should have 1 overriding child scope");

    RuleScope subRuleScope = ootbRule.getOverridingChildScopes(0);
    assertTrue(subRuleScope.hasEnvironmentScope());
    assertTrue(subRuleScope.getEnvironmentScope().getEnvironmentIdsList().contains("env-789"));

    // Verify second rule (Input Explosion) has no overrides
    AiAppRule inputExplosionRule =
        result.stream()
            .filter(rule -> "rule-input-explosion".equals(rule.getRuleId()))
            .findFirst()
            .orElseThrow();

    assertFalse(
        inputExplosionRule.getOverriddenDefault(), "Input explosion rule should not be overridden");
    assertEquals(
        0,
        inputExplosionRule.getOverridingChildScopesCount(),
        "Should have no overriding child scopes");
  }

  @Test
  void testConvertToAiAppRules_WithCustomerScopeOverrides() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfos();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedAnomalyDetectionConfig();
    List<AiAppCustomRule> customRules = Arrays.asList();
    AnomalyConfigScope configScope = createTestConfigScope();

    // Create unresolved configs with customer-level overrides only
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs =
        createUnresolvedConfigsWithCustomerOverrides();

    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(2, result.size());

    // Verify model governance rule has overridden default but no overriding child scopes
    AiAppRule modelGovernanceRule =
        result.stream()
            .filter(rule -> "rule-model-governance".equals(rule.getRuleId()))
            .findFirst()
            .orElseThrow();

    assertTrue(
        modelGovernanceRule.getOverriddenDefault(), "Rule should be overridden at customer level");
    assertEquals(
        0,
        modelGovernanceRule.getOverridingChildScopesCount(),
        "Should have no overriding child scopes for customer-level config");

    // Verify sub-rule also has overridden default
    assertEquals(1, modelGovernanceRule.getAiAppSubRulesCount());
    AiAppSubRule subRule = modelGovernanceRule.getAiAppSubRules(0);
    assertTrue(subRule.hasOotbRule());
    AiAppOotbRule ootbRule = subRule.getOotbRule();

    assertTrue(ootbRule.getOverriddenDefault(), "Sub-rule should be overridden at customer level");
    assertEquals(
        0,
        ootbRule.getOverridingChildScopesCount(),
        "Sub-rule should have no overriding child scopes for customer-level config");
  }

  @Test
  void testConvertToAiAppRules_WithNoOverrides() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfos();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedAnomalyDetectionConfig();
    List<AiAppCustomRule> customRules = Arrays.asList();
    AnomalyConfigScope configScope = createTestConfigScope();
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs = Arrays.asList(); // Empty - no overrides

    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(2, result.size());

    // Verify no rules have overrides
    for (AiAppRule rule : result) {
      assertFalse(
          rule.getOverriddenDefault(), "Rule should not be overridden when no unresolved configs");
      assertEquals(
          0,
          rule.getOverridingChildScopesCount(),
          "Should have no overriding child scopes when no environment configs");

      // Verify sub-rules also have no overrides
      for (AiAppSubRule subRule : rule.getAiAppSubRulesList()) {
        if (subRule.hasOotbRule()) {
          AiAppOotbRule ootbRule = subRule.getOotbRule();
          assertFalse(
              ootbRule.getOverriddenDefault(),
              "Sub-rule should not be overridden when no unresolved configs");
          assertEquals(
              0,
              ootbRule.getOverridingChildScopesCount(),
              "Sub-rule should have no overriding child scopes when no environment configs");
        }
      }
    }
  }

  private List<ScopedAnomalyDetectionConfig> createUnresolvedConfigsWithOverrides() {
    // Create customer-level override for model governance rule (for overridden_default = true)
    AnomalySubRuleConfig subRuleOverride =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("sub-rule-model-governance")
            .setInternal(false) // Override internal flag
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .build();

    Map<String, AnomalySubRuleConfig> subRuleConfigMap = new HashMap<>();
    subRuleConfigMap.put("sub-rule-model-governance", subRuleOverride);

    GenAiAnomalyDetectionConfig genAiConfigCustomer =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("rule-model-governance")
            .setLlmModelGovernance(LlmModelGovernanceAnomalyDetectionConfig.newBuilder().build())
            .setSubRuleConfigs(
                AnomalySubRuleConfigMap.newBuilder().putAllSubRuleConfigs(subRuleConfigMap).build())
            .build();

    AnomalyDetectionConfig detectionConfigCustomer =
        AnomalyDetectionConfig.newBuilder()
            .setGenAiAnomalyDetectionConfig(genAiConfigCustomer)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .build();

    // Customer scope configuration (for overridden_default)
    ScopedAnomalyDetectionConfig customerConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
                    .build())
            .addAnomalyDetectionConfigs(detectionConfigCustomer)
            .build();

    // Environment scope 1 - for overriding child scopes
    GenAiAnomalyDetectionConfig genAiConfigEnv1 =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("rule-model-governance")
            .setLlmModelGovernance(LlmModelGovernanceAnomalyDetectionConfig.newBuilder().build())
            .build();

    AnomalyDetectionConfig detectionConfigEnv1 =
        AnomalyDetectionConfig.newBuilder()
            .setGenAiAnomalyDetectionConfig(genAiConfigEnv1)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
            .build();

    ScopedAnomalyDetectionConfig envConfig1 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setEnvironmentScope(
                        AnomalyEnvironmentScope.newBuilder().setEnvironmentId("env-123").build())
                    .build())
            .addAnomalyDetectionConfigs(detectionConfigEnv1)
            .build();

    // Environment scope 2 - different rule config for overriding child scopes
    GenAiAnomalyDetectionConfig genAiConfigEnv2 =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("rule-model-governance")
            .setLlmModelGovernance(LlmModelGovernanceAnomalyDetectionConfig.newBuilder().build())
            .build();

    AnomalyDetectionConfig detectionConfigEnv2 =
        AnomalyDetectionConfig.newBuilder()
            .setGenAiAnomalyDetectionConfig(genAiConfigEnv2)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .build();

    ScopedAnomalyDetectionConfig envConfig2 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setEnvironmentScope(
                        AnomalyEnvironmentScope.newBuilder().setEnvironmentId("env-456").build())
                    .build())
            .addAnomalyDetectionConfigs(detectionConfigEnv2)
            .build();

    // Environment scope 3 - sub-rule only override for overriding child scopes
    AnomalySubRuleConfig subRuleOnlyOverride =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("sub-rule-model-governance")
            .setInternal(true)
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
            .build();

    Map<String, AnomalySubRuleConfig> subRuleOnlyConfigMap = new HashMap<>();
    subRuleOnlyConfigMap.put("sub-rule-model-governance", subRuleOnlyOverride);

    GenAiAnomalyDetectionConfig genAiConfigEnv3 =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("rule-model-governance")
            .setSubRuleConfigs(
                AnomalySubRuleConfigMap.newBuilder()
                    .putAllSubRuleConfigs(subRuleOnlyConfigMap)
                    .build())
            .build();

    AnomalyDetectionConfig detectionConfigEnv3 =
        AnomalyDetectionConfig.newBuilder().setGenAiAnomalyDetectionConfig(genAiConfigEnv3).build();

    ScopedAnomalyDetectionConfig envConfig3 =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setEnvironmentScope(
                        AnomalyEnvironmentScope.newBuilder().setEnvironmentId("env-789").build())
                    .build())
            .addAnomalyDetectionConfigs(detectionConfigEnv3)
            .build();

    return Arrays.asList(customerConfig, envConfig1, envConfig2, envConfig3);
  }

  private List<ScopedAnomalyDetectionConfig> createUnresolvedConfigsWithCustomerOverrides() {
    // Create customer-level override for model governance rule
    AnomalySubRuleConfig subRuleOverride =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("sub-rule-model-governance")
            .setInternal(false)
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .build();

    Map<String, AnomalySubRuleConfig> subRuleConfigMap = new HashMap<>();
    subRuleConfigMap.put("sub-rule-model-governance", subRuleOverride);

    GenAiAnomalyDetectionConfig genAiConfig =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId("rule-model-governance")
            .setLlmModelGovernance(LlmModelGovernanceAnomalyDetectionConfig.newBuilder().build())
            .setSubRuleConfigs(
                AnomalySubRuleConfigMap.newBuilder().putAllSubRuleConfigs(subRuleConfigMap).build())
            .build();

    AnomalyDetectionConfig detectionConfig =
        AnomalyDetectionConfig.newBuilder()
            .setGenAiAnomalyDetectionConfig(genAiConfig)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .build();

    // Customer scope configuration
    ScopedAnomalyDetectionConfig customerConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
                    .build())
            .addAnomalyDetectionConfigs(detectionConfig)
            .build();

    return Arrays.asList(customerConfig);
  }

  @Test
  void testConvertToAiAppRules_WithCodeDetectedInPromptRule() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfosWithCodeDetection();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedConfigWithCodeDetection();
    List<AiAppCustomRule> customRules = Arrays.asList();
    AnomalyConfigScope configScope = createTestConfigScope();
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs = Arrays.asList();

    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(1, result.size());

    // Verify code detection rule
    AiAppRule codeDetectionRule = result.get(0);
    assertEquals(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID, codeDetectionRule.getRuleId());
    assertEquals("Code Detection Rule", codeDetectionRule.getRuleName());

    // Verify event details
    assertTrue(codeDetectionRule.hasEventDetails());
    EventDetails eventDetails = codeDetectionRule.getEventDetails();
    assertEquals("Code injection detected in prompt", eventDetails.getDescription());
    assertEquals("Block or sanitize the request", eventDetails.getMitigation());
    assertEquals("Potential code injection attack", eventDetails.getImpact());
    assertEquals("https://docs.traceable.ai/code-detection", eventDetails.getReferences());

    // Verify rule status
    assertTrue(codeDetectionRule.hasRuleStatus());
    RuleStatus ruleStatus = codeDetectionRule.getRuleStatus();
    assertFalse(ruleStatus.getDisabled());
    assertFalse(ruleStatus.getInternal());

    // Verify ModSec sub-rules are converted correctly
    assertEquals(2, codeDetectionRule.getAiAppSubRulesCount());

    // Verify first ModSec sub-rule
    AiAppSubRule subRule1 = codeDetectionRule.getAiAppSubRules(0);
    assertTrue(subRule1.hasOotbRule());
    AiAppOotbRule ootbRule1 = subRule1.getOotbRule();
    assertEquals("modsec-rule-1", ootbRule1.getRuleId());
    assertEquals("ModSec SQL Injection Rule", ootbRule1.getRuleName());

    // Verify rule action (defaults when no sub-rule config)
    assertEquals(RuleAction.RULE_ACTION_MONITOR, ootbRule1.getRuleAction());
    assertEquals(SeverityLevel.SEVERITY_LEVEL_MEDIUM, ootbRule1.getSeverityLevel());

    // Verify second ModSec sub-rule
    AiAppSubRule subRule2 = codeDetectionRule.getAiAppSubRules(1);
    assertTrue(subRule2.hasOotbRule());
    AiAppOotbRule ootbRule2 = subRule2.getOotbRule();
    assertEquals("modsec-rule-2", ootbRule2.getRuleId());
    assertEquals("ModSec XSS Rule", ootbRule2.getRuleName());

    // Verify rule action (defaults when no sub-rule config)
    assertEquals(RuleAction.RULE_ACTION_MONITOR, ootbRule2.getRuleAction());
    assertEquals(SeverityLevel.SEVERITY_LEVEL_MEDIUM, ootbRule2.getSeverityLevel());

    // Verify no overrides for this test
    assertFalse(codeDetectionRule.getOverriddenDefault());
    assertEquals(0, codeDetectionRule.getOverridingChildScopesCount());
    assertFalse(ootbRule1.getOverriddenDefault());
    assertEquals(0, ootbRule1.getOverridingChildScopesCount());
    assertFalse(ootbRule2.getOverriddenDefault());
    assertEquals(0, ootbRule2.getOverridingChildScopesCount());

    // Verify hidden field is false (no hidden rules configured)
    assertFalse(codeDetectionRule.getHidden());
    assertFalse(ootbRule1.getHidden());
    assertFalse(ootbRule2.getHidden());
  }

  @Test
  void testConvertToAiAppRules_WithCodeDetectedInPromptRuleAndOverrides() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfosWithCodeDetection();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedConfigWithCodeDetection();
    List<AiAppCustomRule> customRules = Arrays.asList();
    AnomalyConfigScope configScope = createTestConfigScope();
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs =
        createUnresolvedConfigsWithCodeDetectionOverrides();

    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(1, result.size());

    // Verify code detection rule has overrides
    AiAppRule codeDetectionRule = result.get(0);
    assertEquals(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID, codeDetectionRule.getRuleId());

    // Verify main rule override information
    assertTrue(
        codeDetectionRule.getOverriddenDefault(), "Code detection rule should be overridden");
    assertEquals(
        1,
        codeDetectionRule.getOverridingChildScopesCount(),
        "Should have 1 overriding child scope");

    // Verify overriding child scope
    RuleScope overridingScope = codeDetectionRule.getOverridingChildScopes(0);
    assertTrue(overridingScope.hasEnvironmentScope());
    assertTrue(
        overridingScope
            .getEnvironmentScope()
            .getEnvironmentIdsList()
            .contains("env-code-detection"));

    // Verify ModSec sub-rules have overrides
    assertEquals(2, codeDetectionRule.getAiAppSubRulesCount());

    // Verify first ModSec sub-rule override
    AiAppSubRule subRule1 = codeDetectionRule.getAiAppSubRules(0);
    assertTrue(subRule1.hasOotbRule());
    AiAppOotbRule ootbRule1 = subRule1.getOotbRule();
    assertEquals("modsec-rule-1", ootbRule1.getRuleId());

    assertTrue(ootbRule1.getOverriddenDefault(), "ModSec sub-rule should be overridden");
    assertEquals(
        1,
        ootbRule1.getOverridingChildScopesCount(),
        "ModSec sub-rule should have 1 overriding child scope");

    RuleScope subRuleScope = ootbRule1.getOverridingChildScopes(0);
    assertTrue(subRuleScope.hasEnvironmentScope());
    assertTrue(
        subRuleScope.getEnvironmentScope().getEnvironmentIdsList().contains("env-modsec-override"));
  }

  @Test
  void testConvertToAiAppRules_WithCodeDetectedInPromptRuleEmptyThreatRules() {
    // Arrange - Test with empty threat rule configs
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfosWithCodeDetection();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedConfigWithEmptyCodeDetection();
    List<AiAppCustomRule> customRules = Arrays.asList();
    AnomalyConfigScope configScope = createTestConfigScope();
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs = Arrays.asList();

    // Act
    List<AiAppRule> result =
        anomalyToAiAppRuleConverter.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(1, result.size());

    // Verify code detection rule with no ModSec sub-rules
    AiAppRule codeDetectionRule = result.get(0);
    assertEquals(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID, codeDetectionRule.getRuleId());
    assertEquals("Code Detection Rule", codeDetectionRule.getRuleName());

    // Should have no sub-rules since threat_rule_configs is empty
    assertEquals(
        0,
        codeDetectionRule.getAiAppSubRulesCount(),
        "Should have no ModSec sub-rules when threat_rule_configs is empty");
  }

  private List<AnomalyRuleInfo> createTestAnomalyRuleInfosWithCodeDetection() {
    // Create main code detection rule info
    AnomalyEventDetails codeDetectionEventDetails =
        AnomalyEventDetails.newBuilder()
            .setDescription("Code injection detected in prompt")
            .setMitigation("Block or sanitize the request")
            .setImpact("Potential code injection attack")
            .setReferences("https://docs.traceable.ai/code-detection")
            .build();

    AnomalyRuleInfo codeDetectionRuleInfo =
        AnomalyRuleInfo.newBuilder()
            .setRuleId(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID)
            .setRuleName("Code Detection Rule")
            .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_GEN_AI)
            .setEventDetails(codeDetectionEventDetails)
            .putEventLabels("category", "security")
            .putEventLabels("type", "code_injection")
            .build();

    // Create ModSec rule infos (used as sub-rules)
    AnomalyEventDetails modsecEventDetails1 =
        AnomalyEventDetails.newBuilder()
            .setDescription("SQL injection pattern detected")
            .setMitigation("Block SQL injection attempts")
            .setImpact("Database compromise")
            .setReferences("https://docs.traceable.ai/modsec-sql")
            .build();

    AnomalyRuleInfo modsecRuleInfo1 =
        AnomalyRuleInfo.newBuilder()
            .setRuleId("modsec-rule-1")
            .setRuleName("ModSec SQL Injection Rule")
            .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
            .setEventDetails(modsecEventDetails1)
            .putEventLabels("modsec_category", "sql_injection")
            .putEventLabels("severity", "high")
            .build();

    AnomalyEventDetails modsecEventDetails2 =
        AnomalyEventDetails.newBuilder()
            .setDescription("XSS pattern detected")
            .setMitigation("Block XSS attempts")
            .setImpact("Client-side script execution")
            .setReferences("https://docs.traceable.ai/modsec-xss")
            .build();

    AnomalyRuleInfo modsecRuleInfo2 =
        AnomalyRuleInfo.newBuilder()
            .setRuleId("modsec-rule-2")
            .setRuleName("ModSec XSS Rule")
            .setEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
            .setEventDetails(modsecEventDetails2)
            .putEventLabels("modsec_category", "xss")
            .putEventLabels("severity", "medium")
            .build();

    return Arrays.asList(codeDetectionRuleInfo, modsecRuleInfo1, modsecRuleInfo2);
  }

  private ScopedAnomalyDetectionConfig createTestScopedConfigWithCodeDetection() {
    // Create CodeDetectedInPromptThreatRuleConfig for ModSec rules
    CodeDetectedInPromptThreatRuleConfig threatRuleConfig1 =
        CodeDetectedInPromptThreatRuleConfig.newBuilder()
            .setThreatRuleId("modsec-rule-1")
            .setSubRuleIds(StringList.newBuilder().addValues("sub-rule-sql").build())
            .build();

    CodeDetectedInPromptThreatRuleConfig threatRuleConfig2 =
        CodeDetectedInPromptThreatRuleConfig.newBuilder()
            .setThreatRuleId("modsec-rule-2")
            .setSubRuleIds(StringList.newBuilder().addValues("sub-rule-xss").build())
            .build();

    // Create CodeDetectedInPromptAnomalyDetectionConfig
    CodeDetectedInPromptAnomalyDetectionConfig codeDetectedConfig =
        CodeDetectedInPromptAnomalyDetectionConfig.newBuilder()
            .addThreatRuleConfigs(threatRuleConfig1)
            .addThreatRuleConfigs(threatRuleConfig2)
            .build();

    // Create GenAiAnomalyDetectionConfig with code detection
    GenAiAnomalyDetectionConfig genAiConfig =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID)
            .setCodeDetectedInPrompt(codeDetectedConfig)
            .build();

    // Create AnomalyDetectionConfig
    AnomalyDetectionConfig detectionConfig =
        AnomalyDetectionConfig.newBuilder().setGenAiAnomalyDetectionConfig(genAiConfig).build();

    // Create ScopedAnomalyDetectionConfig
    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(createTestConfigScope())
        .addAnomalyDetectionConfigs(detectionConfig)
        .build();
  }

  private ScopedAnomalyDetectionConfig createTestScopedConfigWithEmptyCodeDetection() {
    // Create CodeDetectedInPromptAnomalyDetectionConfig with empty threat rule configs
    CodeDetectedInPromptAnomalyDetectionConfig codeDetectedConfig =
        CodeDetectedInPromptAnomalyDetectionConfig.newBuilder()
            .build(); // Empty threat_rule_configs

    // Create GenAiAnomalyDetectionConfig with empty code detection
    GenAiAnomalyDetectionConfig genAiConfig =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID)
            .setCodeDetectedInPrompt(codeDetectedConfig)
            .build();

    // Create AnomalyDetectionConfig
    AnomalyDetectionConfig detectionConfig =
        AnomalyDetectionConfig.newBuilder().setGenAiAnomalyDetectionConfig(genAiConfig).build();

    // Create ScopedAnomalyDetectionConfig
    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(createTestConfigScope())
        .addAnomalyDetectionConfigs(detectionConfig)
        .build();
  }

  private List<ScopedAnomalyDetectionConfig> createUnresolvedConfigsWithCodeDetectionOverrides() {
    // Create customer-level override for code detection rule
    GenAiAnomalyDetectionConfig genAiConfigCustomer =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID)
            .setCodeDetectedInPrompt(
                CodeDetectedInPromptAnomalyDetectionConfig.newBuilder().build())
            .setSubRuleConfigs(
                AnomalySubRuleConfigMap.newBuilder()
                    .putSubRuleConfigs(
                        "modsec-rule-1",
                        AnomalySubRuleConfig.newBuilder()
                            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR)
                            .build()))
            .build();

    AnomalyDetectionConfig detectionConfigCustomer =
        AnomalyDetectionConfig.newBuilder()
            .setGenAiAnomalyDetectionConfig(genAiConfigCustomer)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(true).build())
            .build();

    // Customer scope configuration (for overridden_default)
    ScopedAnomalyDetectionConfig customerConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
                    .build())
            .addAnomalyDetectionConfigs(detectionConfigCustomer)
            .build();

    // Environment scope configuration (for overriding child scopes)
    GenAiAnomalyDetectionConfig genAiConfigEnv =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID)
            .setCodeDetectedInPrompt(
                CodeDetectedInPromptAnomalyDetectionConfig.newBuilder().build())
            .build();

    AnomalyDetectionConfig detectionConfigEnv =
        AnomalyDetectionConfig.newBuilder()
            .setGenAiAnomalyDetectionConfig(genAiConfigEnv)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
            .build();

    ScopedAnomalyDetectionConfig envConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setEnvironmentScope(
                        AnomalyEnvironmentScope.newBuilder()
                            .setEnvironmentId("env-code-detection")
                            .build())
                    .build())
            .addAnomalyDetectionConfigs(detectionConfigEnv)
            .build();

    // Environment scope for ModSec sub-rule override
    AnomalySubRuleConfig modsecSubRuleOverride =
        AnomalySubRuleConfig.newBuilder()
            .setSubRuleId("modsec-rule-1")
            .setInternal(false)
            .setAnomalyRuleAction(AnomalyRuleAction.ANOMALY_RULE_ACTION_BLOCK)
            .setConfigStatus(AnomalyConfigStatusChange.newBuilder().setDisabled(false).build())
            .build();

    Map<String, AnomalySubRuleConfig> modsecSubRuleConfigMap = new HashMap<>();
    modsecSubRuleConfigMap.put("modsec-rule-1", modsecSubRuleOverride);

    GenAiAnomalyDetectionConfig genAiConfigModsecEnv =
        GenAiAnomalyDetectionConfig.newBuilder()
            .setAnomalyRuleId(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID)
            .setSubRuleConfigs(
                AnomalySubRuleConfigMap.newBuilder()
                    .putAllSubRuleConfigs(modsecSubRuleConfigMap)
                    .build())
            .build();

    AnomalyDetectionConfig detectionConfigModsecEnv =
        AnomalyDetectionConfig.newBuilder()
            .setGenAiAnomalyDetectionConfig(genAiConfigModsecEnv)
            .build();

    ScopedAnomalyDetectionConfig modsecEnvConfig =
        ScopedAnomalyDetectionConfig.newBuilder()
            .setConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setEnvironmentScope(
                        AnomalyEnvironmentScope.newBuilder()
                            .setEnvironmentId("env-modsec-override")
                            .build())
                    .build())
            .addAnomalyDetectionConfigs(detectionConfigModsecEnv)
            .build();

    return Arrays.asList(customerConfig, envConfig, modsecEnvConfig);
  }

  @Test
  void testConvertToAiAppRules_WithHiddenRulesFromFeatureFlags() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfosWithCodeDetection();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedConfigWithCodeDetection();
    List<AiAppCustomRule> customRules = Arrays.asList();
    AnomalyConfigScope configScope = createTestConfigScope();
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs = Arrays.asList();

    // Create a new converter with mock that returns specific hidden rule IDs
    ai.traceable.config.service.feature.caching.client.FeatureCachingClient
        mockFeatureCachingClient =
            Mockito.mock(
                ai.traceable.config.service.feature.caching.client.FeatureCachingClient.class);

    // Configure mock to return specific hidden rule IDs
    Set<String> hiddenRuleIds = Set.of(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID, "modsec-rule-1");
    when(mockFeatureCachingClient.getHiddenDefenseAiFeatures(any(RequestContext.class)))
        .thenReturn(hiddenRuleIds);

    AnomalyToAiAppRuleConverter converterWithHiddenRules =
        new AnomalyToAiAppRuleConverter(mockFeatureCachingClient);

    // Act
    List<AiAppRule> result =
        converterWithHiddenRules.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(1, result.size());

    // Verify code detection rule is marked as hidden
    AiAppRule codeDetectionRule = result.get(0);
    assertEquals(CODE_DETECTED_IN_PROMPT_THREAT_TYPE_ID, codeDetectionRule.getRuleId());
    assertTrue(codeDetectionRule.getHidden(), "Main rule should be hidden based on feature flag");

    // Verify ModSec sub-rules hidden status
    assertEquals(2, codeDetectionRule.getAiAppSubRulesCount());

    // Verify first ModSec sub-rule (modsec-rule-1) is hidden
    AiAppSubRule subRule1 = codeDetectionRule.getAiAppSubRules(0);
    assertTrue(subRule1.hasOotbRule());
    AiAppOotbRule ootbRule1 = subRule1.getOotbRule();
    assertEquals("modsec-rule-1", ootbRule1.getRuleId());
    assertTrue(
        ootbRule1.getHidden(),
        "ModSec sub-rule modsec-rule-1 should be hidden based on feature flag");

    // Verify second ModSec sub-rule (modsec-rule-2) is NOT hidden
    AiAppSubRule subRule2 = codeDetectionRule.getAiAppSubRules(1);
    assertTrue(subRule2.hasOotbRule());
    AiAppOotbRule ootbRule2 = subRule2.getOotbRule();
    assertEquals("modsec-rule-2", ootbRule2.getRuleId());
    assertFalse(ootbRule2.getHidden(), "ModSec sub-rule modsec-rule-2 should NOT be hidden");
  }

  @Test
  void testConvertToAiAppRules_WithHiddenGenAiRulesAndSubRules() {
    // Arrange
    List<AnomalyRuleInfo> anomalyRuleInfos = createTestAnomalyRuleInfos();
    ScopedAnomalyDetectionConfig scopedConfig = createTestScopedAnomalyDetectionConfig();
    List<AiAppCustomRule> customRules = Arrays.asList();
    AnomalyConfigScope configScope = createTestConfigScope();
    List<ScopedAnomalyDetectionConfig> unresolvedConfigs = Arrays.asList();

    // Create a new converter with mock that returns specific hidden rule IDs
    ai.traceable.config.service.feature.caching.client.FeatureCachingClient
        mockFeatureCachingClient =
            Mockito.mock(
                ai.traceable.config.service.feature.caching.client.FeatureCachingClient.class);

    // Configure mock to return specific hidden rule IDs (main rule and sub-rule)
    Set<String> hiddenRuleIds = Set.of("rule-model-governance", "sub-rule-model-governance");
    when(mockFeatureCachingClient.getHiddenDefenseAiFeatures(any(RequestContext.class)))
        .thenReturn(hiddenRuleIds);

    AnomalyToAiAppRuleConverter converterWithHiddenRules =
        new AnomalyToAiAppRuleConverter(mockFeatureCachingClient);

    // Act
    List<AiAppRule> result =
        converterWithHiddenRules.convertToAiAppRules(
            mockRequestContext,
            configScope,
            scopedConfig,
            anomalyRuleInfos,
            customRules,
            unresolvedConfigs);

    // Assert
    assertNotNull(result);
    assertEquals(2, result.size());

    // Verify model governance rule is marked as hidden
    AiAppRule modelGovernanceRule =
        result.stream()
            .filter(rule -> "rule-model-governance".equals(rule.getRuleId()))
            .findFirst()
            .orElseThrow();
    assertTrue(
        modelGovernanceRule.getHidden(),
        "Model governance rule should be hidden based on feature flag");

    // Verify its sub-rule is also hidden
    assertEquals(1, modelGovernanceRule.getAiAppSubRulesCount());
    AiAppSubRule subRule = modelGovernanceRule.getAiAppSubRules(0);
    assertTrue(subRule.hasOotbRule());
    AiAppOotbRule ootbRule = subRule.getOotbRule();
    assertEquals("sub-rule-model-governance", ootbRule.getRuleId());
    assertTrue(ootbRule.getHidden(), "Sub-rule should be hidden based on feature flag");

    // Verify input explosion rule is NOT hidden
    AiAppRule inputExplosionRule =
        result.stream()
            .filter(rule -> "rule-input-explosion".equals(rule.getRuleId()))
            .findFirst()
            .orElseThrow();
    assertFalse(inputExplosionRule.getHidden(), "Input explosion rule should NOT be hidden");
  }
}
