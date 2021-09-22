package org.hypertrace.label.application.rule.config.service;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.CompositeCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Condition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.JsonCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.LeafCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.StringCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.UnaryCondition;
import org.junit.jupiter.api.Test;

public class LabelApplicationRuleValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant-1");
  private final LabelApplicationRuleValidator labelApplicationRuleValidator;
  private final StringCondition errorKeyCondition;
  private final StringCondition correctKeyCondition;
  private final StringCondition correctAuthKeyCondition;
  private final StringCondition correctStringValueCondition;
  private final UnaryCondition errorUnaryValueCondition;
  private final UnaryCondition correctUnaryValueCondition;
  private final JsonCondition correctJsonValueCondition;

  public LabelApplicationRuleValidatorTest() {
    labelApplicationRuleValidator = new LabelApplicationRuleValidatorImpl();
    errorKeyCondition =
        StringCondition.newBuilder()
            .setOperator(StringCondition.Operator.OPERATOR_UNSPECIFIED)
            .setValue("foo")
            .build();
    // The below condition informs that key=foo
    correctKeyCondition =
        StringCondition.newBuilder()
            .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
            .setValue("foo")
            .build();
    errorUnaryValueCondition =
        UnaryCondition.newBuilder()
            .setOperator(UnaryCondition.Operator.OPERATOR_UNSPECIFIED)
            .build();
    correctUnaryValueCondition =
        UnaryCondition.newBuilder().setOperator(UnaryCondition.Operator.OPERATOR_EXISTS).build();
    correctStringValueCondition =
        StringCondition.newBuilder()
            .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
            .setValue("bar")
            .build();
    correctAuthKeyCondition =
        StringCondition.newBuilder()
            .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
            .setValue("auth")
            .build();
    correctJsonValueCondition =
        JsonCondition.newBuilder()
            .setJsonPath("req.http.headers")
            .setStringCondition(
                StringCondition.newBuilder()
                    .setOperator(StringCondition.Operator.OPERATOR_EQUALS)
                    .setValue("valid")
                    .build())
            .build();
  }

  @Test
  void validateOrThrowCreateRuleKeyError() {
    LeafCondition errorLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(errorKeyCondition)
            .setUnaryCondition(correctUnaryValueCondition)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(errorLeafCondition).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest("Wrong Key Rule", matchingCondition);
    Throwable exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request);
            });
    System.out.println(exception.getMessage());
  }

  @Test
  void validateOrThrowCreateRuleValueError() {
    LeafCondition errorLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setUnaryCondition(errorUnaryValueCondition)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(errorLeafCondition).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest("Wrong Value Rule", matchingCondition);
    Throwable exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request);
            });
    System.out.println(exception.getMessage());
  }

  @Test
  void validateOrThrowCreateRuleActionError() {
    LeafCondition errorLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setStringCondition(correctStringValueCondition)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(errorLeafCondition).build();
    CreateLabelApplicationRuleRequest noActionRequest =
        buildCreateLabelApplicationRuleRequestNoAction("No Action Rule", matchingCondition);
    Throwable exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, noActionRequest);
            });
  }

  @Test
  void validateOrThrowCreateRuleLeafCondition() {
    // This will check the condition that foo(key) = bar(value)
    LeafCondition errorLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setStringCondition(correctStringValueCondition)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(errorLeafCondition).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest("Correct Leaf Rule", matchingCondition);
    labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  @Test
  void validateOrThrowCreateRuleCompositeCondition() {
    // This condition implies foo(key) exists AND foo(key) = bar(value) AND
    // req.http.headers.auth(key) = valid(value)
    LeafCondition leafCondition1 =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setUnaryCondition(correctUnaryValueCondition)
            .build();
    Condition condition1 = Condition.newBuilder().setLeafCondition(leafCondition1).build();

    LeafCondition leafCondition2 =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setStringCondition(correctStringValueCondition)
            .build();
    Condition condition2 = Condition.newBuilder().setLeafCondition(leafCondition2).build();

    LeafCondition leafCondition3 =
        LeafCondition.newBuilder()
            .setKeyCondition(correctAuthKeyCondition)
            .setJsonCondition(correctJsonValueCondition)
            .build();
    Condition condition3 = Condition.newBuilder().setLeafCondition(leafCondition3).build();

    List<Condition> conditionList = Arrays.asList(condition1, condition2, condition3);

    CompositeCondition compositeCondition1 =
        CompositeCondition.newBuilder()
            .setOperator(CompositeCondition.LogicalOperator.LOGICAL_OPERATOR_AND)
            .addAllChildren(conditionList)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setCompositeCondition(compositeCondition1).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest("Correct Composite Rule", matchingCondition);
    labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  private CreateLabelApplicationRuleRequest buildCreateCreateLabelApplicationRuleRequest(
      String name, Condition matchingCondition) {
    LabelApplicationRuleData data =
        LabelApplicationRuleData.newBuilder()
            .setName(name)
            .setMatchingCondition(matchingCondition)
            .setLabelAction(buildAction())
            .build();
    CreateLabelApplicationRuleRequest createLabelApplicationRuleRequest =
        CreateLabelApplicationRuleRequest.newBuilder().setData(data).build();
    return createLabelApplicationRuleRequest;
  }

  private CreateLabelApplicationRuleRequest buildCreateLabelApplicationRuleRequestNoAction(
      String name, Condition matchingCondition) {
    LabelApplicationRuleData data =
        LabelApplicationRuleData.newBuilder()
            .setName(name)
            .setMatchingCondition(matchingCondition)
            .build();
    CreateLabelApplicationRuleRequest createLabelApplicationRuleRequest =
        CreateLabelApplicationRuleRequest.newBuilder().setData(data).build();
    return createLabelApplicationRuleRequest;
  }

  private LabelApplicationRuleData.Action buildAction() {
    return LabelApplicationRuleData.Action.newBuilder()
        .setEntityType("API")
        .setOperation(LabelApplicationRuleData.Action.Operation.OPERATION_MERGE)
        .setStaticLabelId("expensive")
        .build();
  }
}
