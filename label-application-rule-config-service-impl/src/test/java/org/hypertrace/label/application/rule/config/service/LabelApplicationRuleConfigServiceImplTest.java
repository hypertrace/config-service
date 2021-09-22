package org.hypertrace.label.application.rule.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleResponse;
import org.hypertrace.label.application.rule.config.service.v1.DeleteLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRuleResponse;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRulesRequest;
import org.hypertrace.label.application.rule.config.service.v1.GetLabelApplicationRulesResponse;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRule;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleConfigServiceGrpc;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleConfigServiceGrpc.LabelApplicationRuleConfigServiceBlockingStub;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData;
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

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    Channel channel = mockGenericConfigService.channel();
    mockGenericConfigService.addService(new LabelApplicationRuleConfigServiceImpl(channel)).start();
    labelApplicationRuleConfigServiceBlockingStub =
        LabelApplicationRuleConfigServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void createLabelApplicationRule() {
    Map<String, LabelApplicationRule> createdRulesById = createRules();
    Set<LabelApplicationRuleData> createdData =
        createdRulesById.values().stream()
            .map(LabelApplicationRule::getData)
            .collect(Collectors.toSet());
    Set<LabelApplicationRuleData> expectedData = new HashSet<>();
    expectedData.add(buildCompositeRuleData());
    expectedData.add(buildSimpleRuleData("auth", "valid"));
    assertEquals(expectedData, createdData);
  }

  @Test
  void getLabelApplicationRule() {
    Map<String, LabelApplicationRule> createdRulesById = createRules();
    createdRulesById
        .keySet()
        .forEach(
            id -> {
              System.out.println(id);
              GetLabelApplicationRuleRequest request =
                  GetLabelApplicationRuleRequest.newBuilder().setId(id).build();
              GetLabelApplicationRuleResponse response =
                  labelApplicationRuleConfigServiceBlockingStub.getLabelApplicationRule(request);
              assertEquals(createdRulesById.get(id), response.getLabelApplicationRule());
            });
  }

  @Test
  void getLabelApplicationRuleError() {
    GetLabelApplicationRuleRequest request =
        GetLabelApplicationRuleRequest.newBuilder().setId("1").build();
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              labelApplicationRuleConfigServiceBlockingStub.getLabelApplicationRule(request);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void getLabelApplicationRules() {
    Map<String, LabelApplicationRule> createdRulesById = createRules();
    Set<LabelApplicationRule> expectedRules =
        createdRulesById.values().stream().collect(Collectors.toUnmodifiableSet());
    GetLabelApplicationRulesResponse response =
        labelApplicationRuleConfigServiceBlockingStub.getLabelApplicationRules(
            GetLabelApplicationRulesRequest.getDefaultInstance());
    assertEquals(
        expectedRules,
        response.getLabelApplicationRulesList().stream().collect(Collectors.toUnmodifiableSet()));
  }

  @Test
  void updateLabelApplicationRule() {
    Map<String, LabelApplicationRule> createdRulesById = createRules();
    LabelApplicationRuleData expectedData = buildSimpleRuleData("auth", "not-valid");
    String updateRuleId = createdRulesById.keySet().stream().findAny().orElse("default");
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
  void updateLabelApplicationRuleError() {
    Map<String, LabelApplicationRule> createdRulesById = createRules();
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
    Map<String, LabelApplicationRule> createdRulesById = createRules();
    createdRulesById
        .keySet()
        .forEach(
            id -> {
              System.out.println(id);
              DeleteLabelApplicationRuleRequest request =
                  DeleteLabelApplicationRuleRequest.newBuilder().setId(id).build();
              labelApplicationRuleConfigServiceBlockingStub.deleteLabelApplicationRule(request);
              List<LabelApplicationRule> rulesList =
                  labelApplicationRuleConfigServiceBlockingStub
                      .getLabelApplicationRules(
                          GetLabelApplicationRulesRequest.getDefaultInstance())
                      .getLabelApplicationRulesList();
              assertEquals(false, rulesList.contains(createdRulesById.get(id)));
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
        .build();
  }

  private Map<String, LabelApplicationRule> createRules() {
    Map<String, LabelApplicationRule> createdRulesById = new HashMap<>();

    LabelApplicationRuleData compositeRuleData = buildCompositeRuleData();
    CreateLabelApplicationRuleRequest compositeRuleRequest =
        CreateLabelApplicationRuleRequest.newBuilder().setData(compositeRuleData).build();
    CreateLabelApplicationRuleResponse compositeRuleResponse =
        labelApplicationRuleConfigServiceBlockingStub.createLabelApplicationRule(
            compositeRuleRequest);
    LabelApplicationRule compositeRule = compositeRuleResponse.getLabelApplicationRule();
    createdRulesById.put(compositeRule.getId(), compositeRule);

    LabelApplicationRuleData simpleRuleData = buildSimpleRuleData("auth", "valid");
    CreateLabelApplicationRuleRequest simpleRuleRequest =
        CreateLabelApplicationRuleRequest.newBuilder().setData(simpleRuleData).build();
    CreateLabelApplicationRuleResponse simpleRuleResponse =
        labelApplicationRuleConfigServiceBlockingStub.createLabelApplicationRule(simpleRuleRequest);
    LabelApplicationRule simpleRule = simpleRuleResponse.getLabelApplicationRule();
    createdRulesById.put(simpleRule.getId(), simpleRule);
    return createdRulesById;
  }

  private LabelApplicationRuleData.Action buildAction() {
    return LabelApplicationRuleData.Action.newBuilder()
        .setEntityType("API")
        .setOperation(LabelApplicationRuleData.Action.Operation.OPERATION_MERGE)
        .setStaticLabelId("expensive")
        .build();
  }
}
