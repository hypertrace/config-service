package org.hypertrace.label.application.rule.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleResponse;
import org.hypertrace.label.application.rule.config.service.v1.DeleteLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRulesRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRulesResponse;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRule;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleConfigServiceGrpc;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleConfigServiceGrpc.LabelApplicationRuleConfigServiceBlockingStub;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Action;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Action.Operation;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.CompositeCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Condition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.JsonCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.LeafCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.StringCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.UnaryCondition;
import org.hypertrace.label.application.rule.config.service.v1.UpdateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.UpdateLabelApplicationRuleResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LabelApplicationRuleConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  LabelApplicationRuleConfigServiceBlockingStub labelApplicationRuleConfigServiceBlockingStub;
  LabelApplicationRule systemLabelApplicationRule;

  @BeforeEach
  void setUp() throws InvalidProtocolBufferException {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    Channel channel = mockGenericConfigService.channel();
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    systemLabelApplicationRule =
        LabelApplicationRule.newBuilder()
            .setId("system-label-rule-id-1")
            .setData(LabelApplicationRuleData.newBuilder().setName("SystemLabelApplicationRule1"))
            .build();
    LabelApplicationRuleConfig config = mock(LabelApplicationRuleConfig.class);
    when(config.getSystemLabelApplicationRules()).thenReturn(List.of(systemLabelApplicationRule));
    when(config.getSystemLabelApplicationRule(systemLabelApplicationRule.getId()))
        .thenReturn(Optional.of(systemLabelApplicationRule));
    when(config.getMaxDynamicLabelApplicationRulesAllowed()).thenReturn(2);
    mockGenericConfigService
        .addService(
            new LabelApplicationRuleConfigServiceImpl(channel, config, configChangeEventGenerator))
        .start();
    labelApplicationRuleConfigServiceBlockingStub =
        LabelApplicationRuleConfigServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void createLabelApplicationRule() {
    LabelApplicationRule simpleRule = createSimpleRule("auth", "valid");
    LabelApplicationRule compositeRule = createCompositeRule();
    List<LabelApplicationRule> createdRules = List.of(simpleRule, compositeRule);
    List<LabelApplicationRuleData> createdData =
        createdRules.stream().map(LabelApplicationRule::getData).collect(Collectors.toList());
    List<LabelApplicationRuleData> expectedData =
        Arrays.asList(buildSimpleRuleData("auth", "valid"), buildCompositeRuleData());
    assertEquals(expectedData, createdData);
  }

  @Test
  void createLabelApplicationRuleWithDynamicLabelApplicationRulesLimitReached() {
    LabelApplicationRuleData simpleRuleData1 = buildSimpleRuleData("auth-1", "valid");
    LabelApplicationRuleData simpleRuleData2 = buildSimpleRuleData("auth-2", "valid");
    LabelApplicationRuleData simpleRuleData3 = buildSimpleRuleData("auth-3", "valid");
    CreateLabelApplicationRuleRequest request1 =
        CreateLabelApplicationRuleRequest.newBuilder().setData(simpleRuleData1).build();
    CreateLabelApplicationRuleResponse response1 =
        labelApplicationRuleConfigServiceBlockingStub.createLabelApplicationRule(request1);
    assertEquals(simpleRuleData1, response1.getLabelApplicationRule().getData());
    CreateLabelApplicationRuleRequest request2 =
        CreateLabelApplicationRuleRequest.newBuilder().setData(simpleRuleData2).build();
    CreateLabelApplicationRuleResponse response2 =
        labelApplicationRuleConfigServiceBlockingStub.createLabelApplicationRule(request2);
    assertEquals(simpleRuleData2, response2.getLabelApplicationRule().getData());
    CreateLabelApplicationRuleRequest request3 =
        CreateLabelApplicationRuleRequest.newBuilder().setData(simpleRuleData3).build();
    assertThrows(
        RuntimeException.class,
        () -> labelApplicationRuleConfigServiceBlockingStub.createLabelApplicationRule(request3));
  }

  @Test
  void getLabelApplicationRules() {
    LabelApplicationRule simpleRule = createSimpleRule("auth", "valid");
    LabelApplicationRule compositeRule = createCompositeRule();
    Set<LabelApplicationRule> expectedRules =
        Set.of(simpleRule, compositeRule, systemLabelApplicationRule);
    GetLabelApplicationRulesResponse response =
        labelApplicationRuleConfigServiceBlockingStub.getLabelApplicationRules(
            GetLabelApplicationRulesRequest.getDefaultInstance());
    assertEquals(
        expectedRules,
        response.getLabelApplicationRulesList().stream().collect(Collectors.toUnmodifiableSet()));
  }

  @Test
  void updateLabelApplicationRule() {
    LabelApplicationRule simpleRule = createSimpleRule("auth", "valid");
    LabelApplicationRuleData expectedData = buildSimpleRuleData("auth", "not-valid");
    String updateRuleId = simpleRule.getId();
    UpdateLabelApplicationRuleRequest request =
        UpdateLabelApplicationRuleRequest.newBuilder()
            .setId(updateRuleId)
            .setData(expectedData)
            .build();
    UpdateLabelApplicationRuleResponse response =
        labelApplicationRuleConfigServiceBlockingStub.updateLabelApplicationRule(request);
    assertEquals(expectedData, response.getLabelApplicationRule().getData());
  }

  @Test
  void updateSystemLabelApplicationRule() {
    LabelApplicationRuleData expectedData = buildSimpleRuleData("auth", "not-valid");
    UpdateLabelApplicationRuleRequest request =
        UpdateLabelApplicationRuleRequest.newBuilder()
            .setId(systemLabelApplicationRule.getId())
            .setData(expectedData)
            .build();
    UpdateLabelApplicationRuleResponse response =
        labelApplicationRuleConfigServiceBlockingStub.updateLabelApplicationRule(request);
    assertEquals(systemLabelApplicationRule.getId(), response.getLabelApplicationRule().getId());
    assertEquals(expectedData, response.getLabelApplicationRule().getData());
  }

  @Test
  void updateLabelApplicationRuleError() {
    LabelApplicationRule simpleRule = createSimpleRule("auth", "valid");
    LabelApplicationRuleData expectedData = buildSimpleRuleData("auth", "not-valid");
    UpdateLabelApplicationRuleRequest request =
        UpdateLabelApplicationRuleRequest.newBuilder().setId("1").setData(expectedData).build();
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              labelApplicationRuleConfigServiceBlockingStub.updateLabelApplicationRule(request);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void deleteApplicationRule() {
    LabelApplicationRule simpleRule = createSimpleRule("auth", "valid");
    LabelApplicationRule compositeRule = createCompositeRule();
    List<LabelApplicationRule> rules = List.of(simpleRule, compositeRule);
    rules.forEach(
        (existingRule) -> {
          DeleteLabelApplicationRuleRequest request =
              DeleteLabelApplicationRuleRequest.newBuilder().setId(existingRule.getId()).build();
          labelApplicationRuleConfigServiceBlockingStub.deleteLabelApplicationRule(request);
          List<LabelApplicationRule> rulesList =
              labelApplicationRuleConfigServiceBlockingStub
                  .getLabelApplicationRules(GetLabelApplicationRulesRequest.getDefaultInstance())
                  .getLabelApplicationRulesList();
          assertFalse(rulesList.contains(existingRule));
        });
  }

  @Test
  void deleteApplicationRuleError() {
    DeleteLabelApplicationRuleRequest request =
        DeleteLabelApplicationRuleRequest.newBuilder().setId("1").build();
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              labelApplicationRuleConfigServiceBlockingStub.deleteLabelApplicationRule(request);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void deleteSystemApplicationRuleError() {
    DeleteLabelApplicationRuleRequest request =
        DeleteLabelApplicationRuleRequest.newBuilder()
            .setId(systemLabelApplicationRule.getId())
            .build();
    assertDoesNotThrow(
        () -> labelApplicationRuleConfigServiceBlockingStub.deleteLabelApplicationRule(request));
    // deleting an already deleted system label application rule should throw error
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                labelApplicationRuleConfigServiceBlockingStub.deleteLabelApplicationRule(request));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void updateDeletedSystemLabelApplicationRule() {
    DeleteLabelApplicationRuleRequest request =
        DeleteLabelApplicationRuleRequest.newBuilder()
            .setId(systemLabelApplicationRule.getId())
            .build();
    assertDoesNotThrow(
        () -> labelApplicationRuleConfigServiceBlockingStub.deleteLabelApplicationRule(request));
    // updating an already deleted system label application rule should throw error
    LabelApplicationRuleData expectedData = buildSimpleRuleData("auth", "not-valid");
    UpdateLabelApplicationRuleRequest updateRequest =
        UpdateLabelApplicationRuleRequest.newBuilder()
            .setId(systemLabelApplicationRule.getId())
            .setData(expectedData)
            .build();
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                labelApplicationRuleConfigServiceBlockingStub.updateLabelApplicationRule(
                    updateRequest));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void getLabelApplicationRulesAfterDeletingSystemLabelApplicationRule() {
    LabelApplicationRule simpleRule = createSimpleRule("auth", "valid");
    LabelApplicationRule compositeRule = createCompositeRule();
    Set<LabelApplicationRule> expectedRules =
        Set.of(simpleRule, compositeRule, systemLabelApplicationRule);
    GetLabelApplicationRulesResponse response =
        labelApplicationRuleConfigServiceBlockingStub.getLabelApplicationRules(
            GetLabelApplicationRulesRequest.getDefaultInstance());
    assertEquals(
        expectedRules,
        response.getLabelApplicationRulesList().stream().collect(Collectors.toUnmodifiableSet()));
    DeleteLabelApplicationRuleRequest request =
        DeleteLabelApplicationRuleRequest.newBuilder()
            .setId(systemLabelApplicationRule.getId())
            .build();
    assertDoesNotThrow(
        () -> labelApplicationRuleConfigServiceBlockingStub.deleteLabelApplicationRule(request));
    expectedRules = Set.of(simpleRule, compositeRule);
    response =
        labelApplicationRuleConfigServiceBlockingStub.getLabelApplicationRules(
            GetLabelApplicationRulesRequest.getDefaultInstance());
    assertEquals(
        expectedRules,
        response.getLabelApplicationRulesList().stream().collect(Collectors.toUnmodifiableSet()));
  }

  private LabelApplicationRuleData buildCompositeRuleData() {
    // This condition implies foo(key) exists AND foo(key) = bar(value) AND
    // req.http.headers.auth(key) = valid(value)
    StringCondition fooKeyCondition =
        StringCondition.newBuilder()
            .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
            .setValue("foo")
            .build();
    UnaryCondition fooUnaryValueCondition =
        UnaryCondition.newBuilder().setOperator(UnaryCondition.Operator.OPERATOR_EXISTS).build();
    StringCondition fooStringValueCondition =
        StringCondition.newBuilder()
            .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
            .setValue("bar")
            .build();

    StringCondition authKeyCondition =
        StringCondition.newBuilder()
            .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
            .setValue("auth")
            .build();
    JsonCondition authJsonValueCondition =
        JsonCondition.newBuilder()
            .setJsonPath("req.http.headers")
            .setStringCondition(
                StringCondition.newBuilder()
                    .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
                    .setValue("valid")
                    .build())
            .build();

    LeafCondition leafCondition1 =
        LeafCondition.newBuilder()
            .setKeyCondition(fooKeyCondition)
            .setUnaryCondition(fooUnaryValueCondition)
            .build();
    Condition condition1 = Condition.newBuilder().setLeafCondition(leafCondition1).build();

    LeafCondition leafCondition2 =
        LeafCondition.newBuilder()
            .setKeyCondition(fooKeyCondition)
            .setStringCondition(fooStringValueCondition)
            .build();
    Condition condition2 = Condition.newBuilder().setLeafCondition(leafCondition2).build();

    LeafCondition leafCondition3 =
        LeafCondition.newBuilder()
            .setKeyCondition(authKeyCondition)
            .setJsonCondition(authJsonValueCondition)
            .build();
    Condition condition3 = Condition.newBuilder().setLeafCondition(leafCondition3).build();

    List<Condition> conditionList = Arrays.asList(condition1, condition2, condition3);

    CompositeCondition compositeCondition1 =
        CompositeCondition.newBuilder()
            .setOperator(
                LabelApplicationRuleData.CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
            .addAllChildren(conditionList)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setCompositeCondition(compositeCondition1).build();
    return LabelApplicationRuleData.newBuilder()
        .setName("Composite rule")
        .setMatchingCondition(matchingCondition)
        .setLabelAction(buildAction())
        .setEnabled(true)
        .build();
  }

  private LabelApplicationRuleData buildSimpleRuleData(String key, String value) {
    StringCondition fooKeyCondition =
        StringCondition.newBuilder()
            .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
            .setValue(key)
            .build();
    StringCondition fooValueCondition =
        StringCondition.newBuilder()
            .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
            .setValue(value)
            .build();
    LeafCondition leafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(fooKeyCondition)
            .setStringCondition(fooValueCondition)
            .build();
    Condition matchingCondition = Condition.newBuilder().setLeafCondition(leafCondition).build();
    return LabelApplicationRuleData.newBuilder()
        .setName("Simple rule")
        .setMatchingCondition(matchingCondition)
        .setLabelAction(buildAction())
        .setEnabled(true)
        .build();
  }

  private LabelApplicationRule createSimpleRule(String key, String value) {
    LabelApplicationRuleData simpleRuleData = buildSimpleRuleData(key, value);
    CreateLabelApplicationRuleRequest simpleRuleRequest =
        CreateLabelApplicationRuleRequest.newBuilder().setData(simpleRuleData).build();
    CreateLabelApplicationRuleResponse simpleRuleResponse =
        labelApplicationRuleConfigServiceBlockingStub.createLabelApplicationRule(simpleRuleRequest);
    return simpleRuleResponse.getLabelApplicationRule();
  }

  private LabelApplicationRule createCompositeRule() {
    Map<String, LabelApplicationRule> createdRulesById = new HashMap<>();

    LabelApplicationRuleData compositeRuleData = buildCompositeRuleData();
    CreateLabelApplicationRuleRequest compositeRuleRequest =
        CreateLabelApplicationRuleRequest.newBuilder().setData(compositeRuleData).build();
    CreateLabelApplicationRuleResponse compositeRuleResponse =
        labelApplicationRuleConfigServiceBlockingStub.createLabelApplicationRule(
            compositeRuleRequest);
    return compositeRuleResponse.getLabelApplicationRule();
  }

  private LabelApplicationRuleData.Action buildAction() {
    return Action.newBuilder()
        .addEntityTypes("API")
        .setOperation(Operation.OPERATION_MERGE)
        .setDynamicLabelKey("key")
        .build();
  }
}
