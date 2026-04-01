package ai.traceable.config.service;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALERT;
import static ai.traceable.detection.exclusion.config.service.v1.KeyMetadata.KEY_METADATA_USER_AGENT;
import static ai.traceable.detection.exclusion.config.service.v1.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DeleteDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionEdgeDecisionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesResponse;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationSeverity;
import ai.traceable.detection.exclusion.config.service.v1.KeyMetadataMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.RuleChangeSource;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.detection.exclusion.config.service.v1.SpanAttributeMatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.UrlScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class DetectionExclusionConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static final String INPUT_DIR = "detection-exclusion/rules/input";
  private static final String EXPECTED_OUTPUT_DIR = "detection-exclusion/rules/expected-output";
  private static final JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();

  private static DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub
      detectionExclusionConfigServiceStub;
  private static final DetectionExclusionRuleInfo RULE_INFO1 =
      DetectionExclusionRuleInfo.newBuilder()
          .setName("ruleName1")
          .addExclusionTargets(EXCLUSION_TARGET_ALERT)
          .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
          .addConditions(
              DetectionExclusionCondition.newBuilder()
                  .setIpReputationCondition(
                      IpReputationCondition.newBuilder()
                          .setMaxIpReputationSeverity(
                              IpReputationSeverity.IP_REPUTATION_SEVERITY_MEDIUM)))
          .addConditions(
              DetectionExclusionCondition.newBuilder()
                  .setEventCondition(
                      EventCondition.newBuilder()
                          .addSystemDefinedEvents(
                              SystemDefinedEvent.newBuilder()
                                  .setEventFamily(
                                      SystemDefinedEventFamily
                                          .SYSTEM_DEFINED_EVENT_FAMILY_API_DEF))))
          .setRuleStatus(
              DetectionExclusionRuleStatus.newBuilder()
                  .setChangeSource(RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER))
          .build();
  private static final DetectionExclusionRuleInfo RULE_INFO2 =
      DetectionExclusionRuleInfo.newBuilder()
          .setName("ruleName2")
          .addExclusionTargets(EXCLUSION_TARGET_ALERT)
          .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
          .addConditions(
              DetectionExclusionCondition.newBuilder()
                  .setEventCondition(
                      EventCondition.newBuilder()
                          .addSystemDefinedEvents(
                              SystemDefinedEvent.newBuilder()
                                  .setEventFamily(
                                      SystemDefinedEventFamily
                                          .SYSTEM_DEFINED_EVENT_FAMILY_MODSEC))))
          .setRuleStatus(
              DetectionExclusionRuleStatus.newBuilder()
                  .setChangeSource(RuleChangeSource.RULE_CHANGE_SOURCE_TRACEABLE))
          .build();
  private static final DetectionExclusionRuleScope RULE_SCOPE1 =
      DetectionExclusionRuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env1").build())
          .build();
  private static final DetectionExclusionRuleScope RULE_SCOPE2 =
      DetectionExclusionRuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env2").build())
          .build();
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenantId");

  @BeforeAll
  static void init() {
    detectionExclusionConfigServiceStub =
        DetectionExclusionConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testCRUDDetectionExclusionRules() {
    List<DetectionExclusionRule> rules = getRules(GetRulesFilter.getDefaultInstance());
    // default rules
    assertEquals(19, rules.size());

    // creating rule1
    CreateDetectionExclusionRuleRequest createRequest1 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleInfo(RULE_INFO1)
            .setRuleScope(RULE_SCOPE1)
            .build();
    DetectionExclusionRule rule1 =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub
                    .createDetectionExclusionRule(createRequest1)
                    .getRule());
    DetectionExclusionRule expectedRule =
        DetectionExclusionRule.newBuilder()
            .setId(rule1.getId())
            .setRuleInfo(RULE_INFO1)
            .setRuleScope(RULE_SCOPE1)
            .build();
    assertEquals(expectedRule, rule1);

    // creating rule2
    CreateDetectionExclusionRuleRequest createRequest2 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleInfo(RULE_INFO2)
            .setRuleScope(RULE_SCOPE2)
            .build();
    DetectionExclusionRule rule2 =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub
                    .createDetectionExclusionRule(createRequest2)
                    .getRule());
    expectedRule =
        DetectionExclusionRule.newBuilder()
            .setId(rule2.getId())
            .setRuleInfo(RULE_INFO2)
            .setRuleScope(RULE_SCOPE2)
            .build();
    assertEquals(expectedRule, rule2);

    // fetching rules with and without filter
    rules = getRules(GetRulesFilter.getDefaultInstance());
    assertTrue(rules.contains(rule1));
    assertTrue(rules.contains(rule2));

    rules = getRules(GetRulesFilter.newBuilder().setRuleScope(RULE_SCOPE1).build());
    assertTrue(rules.contains(rule1));
    assertFalse(rules.contains(rule2));

    rules = getRules(GetRulesFilter.newBuilder().setRuleScope(RULE_SCOPE2).build());
    assertFalse(rules.contains(rule1));
    assertTrue(rules.contains(rule2));

    // updating rule1
    UpdateDetectionExclusionRuleRequest updateRequest =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(rule1.toBuilder().setRuleScope(RULE_SCOPE2))
            .build();
    rule1 =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub
                    .updateDetectionExclusionRule(updateRequest)
                    .getRule());
    expectedRule =
        DetectionExclusionRule.newBuilder()
            .setId(rule1.getId())
            .setRuleInfo(RULE_INFO1)
            .setRuleScope(RULE_SCOPE2)
            .build();
    assertEquals(expectedRule, rule1);

    // deleting rule1
    DeleteDetectionExclusionRuleRequest deleteRequest =
        DeleteDetectionExclusionRuleRequest.newBuilder().setId(rule1.getId()).build();
    REQUEST_CONTEXT.call(
        () -> detectionExclusionConfigServiceStub.deleteDetectionExclusionRule(deleteRequest));
    rules = getRules(GetRulesFilter.getDefaultInstance());
    assertFalse(rules.contains(rule1));
  }

  @ParameterizedTest
  @MethodSource("getInputFileNames")
  void testGetDetectionExclusionEdgeDecisionRules(String fileName)
      throws InvalidProtocolBufferException {
    String inputFileStr = readResourceFileAsString(INPUT_DIR, fileName);
    String expectedOutputFileStr = readResourceFileAsString(EXPECTED_OUTPUT_DIR, fileName);
    DetectionExclusionRule.Builder inputRuleBuilder = DetectionExclusionRule.newBuilder();
    parser.merge(inputFileStr, inputRuleBuilder);
    EdgeDecisionEngineConfig.Builder expectedEdgeDecisionEngineConfigBuilder =
        EdgeDecisionEngineConfig.newBuilder();
    parser.merge(expectedOutputFileStr, expectedEdgeDecisionEngineConfigBuilder);

    // create rule
    CreateDetectionExclusionRuleRequest createRequest =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleInfo(inputRuleBuilder.getRuleInfo())
            .setRuleScope(inputRuleBuilder.getRuleScope())
            .build();
    DetectionExclusionRule rule =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub
                    .createDetectionExclusionRule(createRequest)
                    .getRule());

    // update expected decision rule with rule-id
    EdgeDecisionRule expectedDecisionRule =
        expectedEdgeDecisionEngineConfigBuilder.build().getDecisionRules(0).toBuilder()
            .setId(rule.getId())
            .build();

    GetRulesFilter rulesFilter =
        GetRulesFilter.newBuilder()
            .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_BLOCK)
            .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
            .setDisabled(false)
            .build();
    EdgeDecisionEngineConfig outputEdgeDecisionEngineConfig =
        REQUEST_CONTEXT
            .call(
                () ->
                    detectionExclusionConfigServiceStub.getDetectionExclusionEdgeDecisionRules(
                        GetDetectionExclusionEdgeDecisionRulesRequest.newBuilder()
                            .setFilter(rulesFilter)
                            .build()))
            .getEdgeDecisionEngineConfig();
    assertNotNull(outputEdgeDecisionEngineConfig);
    assertEquals(1, outputEdgeDecisionEngineConfig.getDecisionRulesCount());
    assertEquals(expectedDecisionRule, outputEdgeDecisionEngineConfig.getDecisionRules(0));

    // delete rule to clean up
    DeleteDetectionExclusionRuleRequest deleteRequest =
        DeleteDetectionExclusionRuleRequest.newBuilder().setId(rule.getId()).build();
    REQUEST_CONTEXT.call(
        () -> detectionExclusionConfigServiceStub.deleteDetectionExclusionRule(deleteRequest));
  }

  @Test
  void testGetExclusionModsecRules() {
    String modsecDirectives = "";
    String modsecInitializationRules = "";
    URL directiveUrl =
        Resources.getResource(
            "modsec/crs/directives/modsec-directives-v3-secarglimits-detectiononly-mode.conf");
    URL initializationRulesUrl =
        Resources.getResource("modsec/crs/modsec-initialization-901-rules.conf");
    try {
      modsecDirectives = Resources.toString(directiveUrl, StandardCharsets.UTF_8) + "\n";
      modsecInitializationRules =
          Resources.toString(initializationRulesUrl, StandardCharsets.UTF_8);
    } catch (IOException e) {
      fail("Failed to read modsec directives file and/or modsec initialization rules file.");
    }

    GetRulesFilter getRulesFilter =
        GetRulesFilter.newBuilder()
            .addRuleCreationSources(RuleSource.RULE_SOURCE_TRACEABLE)
            .setRuleScope(
                DetectionExclusionRuleScope.newBuilder()
                    .setEnvironmentScope(
                        EnvironmentScope.newBuilder().addEnvironmentIds("test-getModsec-env")))
            .build();

    List<DetectionExclusionRule> initialRules = getRules(getRulesFilter);
    assertTrue(initialRules.isEmpty());
    List<DetectionExclusionRule> createdRules = createRulesForTestGetExclusionModsecRules();
    assertEquals(2, createdRules.size());

    GetExclusionModsecRulesResponse getExclusionModsecRulesResponse =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub.getExclusionModsecRules(
                    GetExclusionModsecRulesRequest.newBuilder()
                        .setRulesFilter(getRulesFilter)
                        .build()));

    List<String> createdRuleIds =
        createdRules.stream()
            .map(DetectionExclusionRule::getId)
            .collect(Collectors.toUnmodifiableList());

    assertEquals(2, getExclusionModsecRulesResponse.getModsecRulesCount());
    assertTrue(
        createdRuleIds.contains(
            getExclusionModsecRulesResponse.getModsecRules(0).getRule().getId()));
    assertTrue(
        createdRuleIds.contains(
            getExclusionModsecRulesResponse.getModsecRules(1).getRule().getId()));

    assertEquals(
        modsecDirectives + modsecInitializationRules,
        getExclusionModsecRulesResponse.getModsecDirectivesBlob());

    disableCreatedRule(createdRules.get(0));

    getExclusionModsecRulesResponse =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub.getExclusionModsecRules(
                    GetExclusionModsecRulesRequest.newBuilder()
                        .setRulesFilter(getRulesFilter.toBuilder().setDisabled(true).build())
                        .build()));

    assertEquals(1, getExclusionModsecRulesResponse.getModsecRulesCount());
    assertEquals(
        createdRules.get(0).getId(),
        getExclusionModsecRulesResponse.getModsecRules(0).getRule().getId());
    assertEquals(
        modsecDirectives + modsecInitializationRules,
        getExclusionModsecRulesResponse.getModsecDirectivesBlob());

    // deleting the created rules for this test
    for (DetectionExclusionRule rule : createdRules) {
      DeleteDetectionExclusionRuleRequest deleteRequest =
          DeleteDetectionExclusionRuleRequest.newBuilder().setId(rule.getId()).build();
      REQUEST_CONTEXT.call(
          () -> detectionExclusionConfigServiceStub.deleteDetectionExclusionRule(deleteRequest));
    }

    // verifying cleanup
    List<DetectionExclusionRule> finalRules = getRules(getRulesFilter);
    assertEquals(0, finalRules.size());
  }

  private List<DetectionExclusionRule> createRulesForTestGetExclusionModsecRules() {
    DetectionExclusionRuleInfo ruleInfo1 =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("test-exclusion-rule-1")
            .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_BLOCK)
            .addConditions(
                DetectionExclusionCondition.newBuilder()
                    .setScopeCondition(
                        ScopeCondition.newBuilder()
                            .setUrlScope(UrlScope.newBuilder().addUrlRegexes("url-regex"))))
            .setRuleStatus(
                DetectionExclusionRuleStatus.newBuilder()
                    .setRuleCreationSource(RuleSource.RULE_SOURCE_TRACEABLE))
            .build();

    DetectionExclusionRuleScope ruleScope1 =
        DetectionExclusionRuleScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder().addEnvironmentIds("test-getModsec-env"))
            .build();

    CreateDetectionExclusionRuleRequest createRequest1 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleInfo(ruleInfo1)
            .setRuleScope(ruleScope1)
            .build();

    DetectionExclusionRule rule1 =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub
                    .createDetectionExclusionRule(createRequest1)
                    .getRule());

    DetectionExclusionRuleInfo ruleInfo2 =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("test-exclusion-rule-2")
            .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_BLOCK)
            .addConditions(
                DetectionExclusionCondition.newBuilder()
                    .setAttributeMatchCondition(
                        SpanAttributeMatchCondition.newBuilder()
                            .setKeyMatchCondition(
                                KeyMetadataMatchCondition.newBuilder()
                                    .setMetadata(KEY_METADATA_USER_AGENT))
                            .setValueMatchCondition(
                                MatchCondition.newBuilder()
                                    .setOperator(MATCH_OPERATOR_MATCHES_REGEX)
                                    .setValue(Value.newBuilder().setStringValue("str-value-2")))))
            .setRuleStatus(
                DetectionExclusionRuleStatus.newBuilder()
                    .setRuleCreationSource(RuleSource.RULE_SOURCE_TRACEABLE))
            .build();

    DetectionExclusionRuleScope ruleScope2 =
        DetectionExclusionRuleScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder().addEnvironmentIds("test-getModsec-env"))
            .build();

    CreateDetectionExclusionRuleRequest createRequest2 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleInfo(ruleInfo2)
            .setRuleScope(ruleScope2)
            .build();

    DetectionExclusionRule rule2 =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub
                    .createDetectionExclusionRule(createRequest2)
                    .getRule());

    return List.of(rule1, rule2);
  }

  private void disableCreatedRule(DetectionExclusionRule detectionExclusionRule) {
    DetectionExclusionRule updatedRule =
        detectionExclusionRule.toBuilder()
            .setRuleInfo(
                detectionExclusionRule.getRuleInfo().toBuilder()
                    .setRuleStatus(
                        detectionExclusionRule.getRuleInfo().getRuleStatus().toBuilder()
                            .setDisabled(true)
                            .clearRuleCreationSource()
                            .build())
                    .build())
            .build();
    UpdateDetectionExclusionRuleRequest updateRequest =
        UpdateDetectionExclusionRuleRequest.newBuilder().setRule(updatedRule).build();
    REQUEST_CONTEXT.call(
        () -> detectionExclusionConfigServiceStub.updateDetectionExclusionRule(updateRequest));
  }

  static List<String> getInputFileNames() {
    String folderName =
        Objects.requireNonNull(
                DetectionExclusionConfigServiceIntegrationTest.class
                    .getClassLoader()
                    .getResource(INPUT_DIR))
            .getFile();
    File queriesFolder = new File(folderName);

    return Arrays.stream(Objects.requireNonNull(queriesFolder.listFiles()))
        .map(File::getName)
        .collect(Collectors.toUnmodifiableList());
  }

  private String readResourceFileAsString(String dirName, String fileName) {
    try {
      File file =
          new File(
              Objects.requireNonNull(
                      this.getClass()
                          .getClassLoader()
                          .getResource(dirName + File.separator + fileName))
                  .toURI());
      return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<DetectionExclusionRule> getRules(GetRulesFilter rulesFilter) {
    return REQUEST_CONTEXT.call(
        () ->
            detectionExclusionConfigServiceStub
                .getDetectionExclusionRules(
                    GetDetectionExclusionRulesRequest.newBuilder().setFilter(rulesFilter).build())
                .getRulesList());
  }
}
