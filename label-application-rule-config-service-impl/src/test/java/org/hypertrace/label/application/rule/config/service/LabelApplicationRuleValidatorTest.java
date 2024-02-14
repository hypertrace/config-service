package org.hypertrace.label.application.rule.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.label.application.rule.config.service.v1.CreateLabelApplicationRuleRequest;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Action;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.CompositeCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.Condition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.JsonCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.LeafCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.StringKeyCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.StringValueCondition;
import org.hypertrace.label.application.rule.config.service.v1.LabelApplicationRuleData.UnaryCondition;
import org.junit.jupiter.api.Test;

public class LabelApplicationRuleValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant-1");
  private final LabelApplicationRuleValidator labelApplicationRuleValidator;
  private final StringKeyCondition errorKeyCondition;
  private final StringKeyCondition correctKeyCondition;
  private final StringKeyCondition correctKeyConditionMatchesRegex;
  private final StringKeyCondition correctAuthKeyCondition;
  private final StringValueCondition correctStringValueCondition;
  private final StringValueCondition correctRegexStringValueCondition;
  private final StringValueCondition correctRegexNotMatchingCondition;
  private final StringValueCondition incorrectRegexStringValueCondition;
  private final StringValueCondition incorrectRegexNotMatchingCondition;
  private final StringValueCondition correctStringValueConditionWithMatchesIPs;
  private final StringValueCondition incorrectStringValueConditionWithMatchesIPs;
  private final UnaryCondition errorUnaryValueCondition;
  private final UnaryCondition correctUnaryValueCondition;
  private final JsonCondition correctJsonValueCondition;

  public LabelApplicationRuleValidatorTest() {
    labelApplicationRuleValidator = new LabelApplicationRuleValidatorImpl();
    errorKeyCondition =
        StringKeyCondition.newBuilder()
            .setOperator(StringKeyCondition.Operator.OPERATOR_UNSPECIFIED)
            .setValue("foo")
            .build();
    // The below condition informs that key=foo
    correctKeyCondition =
        StringKeyCondition.newBuilder()
            .setOperator(StringKeyCondition.Operator.OPERATOR_EQUALS)
            .setValue("foo")
            .build();
    correctKeyConditionMatchesRegex =
        StringKeyCondition.newBuilder()
            .setOperator(StringKeyCondition.Operator.OPERATOR_MATCHES_REGEX)
            .setValue("foo.*")
            .build();
    errorUnaryValueCondition =
        UnaryCondition.newBuilder()
            .setOperator(UnaryCondition.Operator.OPERATOR_UNSPECIFIED)
            .build();
    correctUnaryValueCondition =
        UnaryCondition.newBuilder().setOperator(UnaryCondition.Operator.OPERATOR_EXISTS).build();
    correctStringValueCondition =
        StringValueCondition.newBuilder()
            .setOperator(StringValueCondition.Operator.OPERATOR_EQUALS)
            .setValue("bar")
            .build();
    correctRegexStringValueCondition =
        StringValueCondition.newBuilder()
            .setOperator(StringValueCondition.Operator.OPERATOR_MATCHES_REGEX)
            .setValue(".*bar.*")
            .build();
    correctRegexNotMatchingCondition =
        StringValueCondition.newBuilder()
            .setOperator(StringValueCondition.Operator.OPERATOR_NOT_MATCHES_REGEX)
            .setValue(".*bar.*")
            .build();
    incorrectRegexStringValueCondition =
        StringValueCondition.newBuilder()
            .setOperator(StringValueCondition.Operator.OPERATOR_MATCHES_REGEX)
            .setValue("((?!bar)")
            .build();
    incorrectRegexNotMatchingCondition =
        StringValueCondition.newBuilder()
            .setOperator(StringValueCondition.Operator.OPERATOR_NOT_MATCHES_REGEX)
            .setValue("((?!bar)")
            .build();
    correctStringValueConditionWithMatchesIPs =
        StringValueCondition.newBuilder()
            .setOperator(StringValueCondition.Operator.OPERATOR_MATCHES_IPS)
            .setValues(
                StringValueCondition.StringList.newBuilder()
                    .addAllValues(List.of("1.2.3.4", "4.5.6.7/8")))
            .build();
    incorrectStringValueConditionWithMatchesIPs =
        StringValueCondition.newBuilder()
            .setOperator(StringValueCondition.Operator.OPERATOR_MATCHES_IPS)
            .setValues(
                StringValueCondition.StringList.newBuilder()
                    .addAllValues(List.of("1.2.3.4/5", "1.2 3.4")))
            .build();
    correctAuthKeyCondition =
        StringKeyCondition.newBuilder()
            .setOperator(StringKeyCondition.Operator.OPERATOR_EQUALS)
            .setValue("auth")
            .build();
    correctJsonValueCondition =
        JsonCondition.newBuilder()
            .setJsonPath("req.http.headers")
            .setStringCondition(
                StringValueCondition.newBuilder()
                    .setOperator(StringValueCondition.Operator.OPERATOR_EQUALS)
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
        buildCreateCreateLabelApplicationRuleRequest("Wrong Key Rule", matchingCondition, null);
    assertThrows(
        StatusRuntimeException.class,
        () -> labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request));
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
        buildCreateCreateLabelApplicationRuleRequest("Wrong Value Rule", matchingCondition, null);
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request));
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
    assertThrows(
        StatusRuntimeException.class,
        () -> labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, noActionRequest));
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
        buildCreateCreateLabelApplicationRuleRequest("Correct Leaf Rule", matchingCondition, null);
    labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  @Test
  void validateOrThrowCreateRuleLeafConditionWithMatchesIPs() {
    // This will check the condition that foo(key) = bar(value)
    LeafCondition errorLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setStringCondition(correctStringValueConditionWithMatchesIPs)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(errorLeafCondition).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest("Correct Leaf Rule", matchingCondition, null);
    labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  @Test
  void validateOrThrowCreateRuleInvalidLeafConditionWithMatchesIPs() {
    // This will check the condition that foo(key) = bar(value)
    LeafCondition errorLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyConditionMatchesRegex)
            .setStringCondition(incorrectStringValueConditionWithMatchesIPs)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(errorLeafCondition).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest("Correct Leaf Rule", matchingCondition, null);

    assertThrows(
        StatusRuntimeException.class,
        () -> labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request));
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
        buildCreateCreateLabelApplicationRuleRequest(
            "Correct Composite Rule", matchingCondition, null);
    labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  @Test
  void validateLabelExpression() {
    LeafCondition errorLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setStringCondition(correctStringValueCondition)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(errorLeafCondition).build();
    CreateLabelApplicationRuleRequest request1 =
        buildCreateCreateLabelApplicationRuleRequest(
            "Label Expression Rule", matchingCondition, "${status}_{wrong-key}");
    assertThrows(
        StatusRuntimeException.class,
        () -> labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request1));
    CreateLabelApplicationRuleRequest request2 =
        buildCreateCreateLabelApplicationRuleRequest(
            "Label Expression Rule", matchingCondition, "${status}_{method}");
    assertDoesNotThrow(
        () -> labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request2));
  }

  @Test
  void validateDisabledRule() {
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
        buildCreateCreateLabelApplicationRuleRequest(
            "Correct Composite Rule", false, matchingCondition, null);
    assertDoesNotThrow(
        () -> labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request));
  }

  @Test
  void validateOrThrowCreateRuleLeafConditionWithMatchRegex() {
    // This will check the condition that foo(key) = bar(value)
    LeafCondition correctLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setStringCondition(correctRegexStringValueCondition)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(correctLeafCondition).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest("Correct Leaf Rule", matchingCondition, null);
    labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  @Test
  void validateOrThrowCreateRuleLeafConditionWithNotMatchRegex() {
    // This will check the condition that foo(key) = bar(value)
    LeafCondition correctLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setStringCondition(correctRegexNotMatchingCondition)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(correctLeafCondition).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest("Correct Leaf Rule", matchingCondition, null);
    labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request);
  }

  @Test
  void validateOrThrowCreateRuleInvalidLeafConditionWithMatchRegex() {
    // This will check the condition that foo(key) = bar(value)
    LeafCondition invalidLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setStringCondition(incorrectRegexStringValueCondition)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(invalidLeafCondition).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest(
            "Incorrect Leaf Rule", matchingCondition, null);

    assertThrows(
        StatusRuntimeException.class,
        () -> labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request));
  }

  @Test
  void validateOrThrowCreateRuleInvalidLeafConditionWithNotMatchRegex() {
    // This will check the condition that foo(key) = bar(value)
    LeafCondition invalidLeafCondition =
        LeafCondition.newBuilder()
            .setKeyCondition(correctKeyCondition)
            .setStringCondition(incorrectRegexNotMatchingCondition)
            .build();
    Condition matchingCondition =
        Condition.newBuilder().setLeafCondition(invalidLeafCondition).build();
    CreateLabelApplicationRuleRequest request =
        buildCreateCreateLabelApplicationRuleRequest(
            "Incorrect Leaf Rule", matchingCondition, null);

    assertThrows(
        StatusRuntimeException.class,
        () -> labelApplicationRuleValidator.validateOrThrow(REQUEST_CONTEXT, request));
  }

  private CreateLabelApplicationRuleRequest buildCreateCreateLabelApplicationRuleRequest(
      String name, Condition matchingCondition, String labelExpression) {
    return buildCreateCreateLabelApplicationRuleRequest(
        name, true, matchingCondition, labelExpression);
  }

  private CreateLabelApplicationRuleRequest buildCreateCreateLabelApplicationRuleRequest(
      String name, boolean enabled, Condition matchingCondition, String labelExpression) {
    LabelApplicationRuleData data;
    if (Objects.nonNull(labelExpression)) {
      data =
          LabelApplicationRuleData.newBuilder()
              .setName(name)
              .setMatchingCondition(matchingCondition)
              .setLabelAction(buildDynamicLabelAction(labelExpression))
              .setEnabled(enabled)
              .build();
    } else {
      data =
          LabelApplicationRuleData.newBuilder()
              .setName(name)
              .setMatchingCondition(matchingCondition)
              .setLabelAction(buildAction())
              .setEnabled(enabled)
              .build();
    }
    return CreateLabelApplicationRuleRequest.newBuilder().setData(data).build();
  }

  private CreateLabelApplicationRuleRequest buildCreateLabelApplicationRuleRequestNoAction(
      String name, Condition matchingCondition) {
    LabelApplicationRuleData data =
        LabelApplicationRuleData.newBuilder()
            .setName(name)
            .setMatchingCondition(matchingCondition)
            .setEnabled(true)
            .build();
    return CreateLabelApplicationRuleRequest.newBuilder().setData(data).build();
  }

  private LabelApplicationRuleData.Action buildAction() {
    return LabelApplicationRuleData.Action.newBuilder()
        .addAllEntityTypes(List.of("API"))
        .setOperation(LabelApplicationRuleData.Action.Operation.OPERATION_MERGE)
        .setDynamicLabelKey("key")
        .build();
  }

  private Action buildDynamicLabelAction(String labelExpression) {
    return LabelApplicationRuleData.Action.newBuilder()
        .addAllEntityTypes(List.of("API"))
        .setOperation(LabelApplicationRuleData.Action.Operation.OPERATION_MERGE)
        .setDynamicLabelExpression(
            LabelApplicationRuleData.Action.DynamicLabel.newBuilder()
                .setLabelExpression(labelExpression)
                .addTokenExtractionRules(buildTokenExtractionRule("req.http", "status", null))
                .addTokenExtractionRules(buildTokenExtractionRule("req.http", "some key", "method"))
                .build())
        .build();
  }

  private Action.DynamicLabel.TokenExtractionRule buildTokenExtractionRule(
      String jsonPath, String key, String alias) {
    if (Objects.nonNull(alias)) {
      return Action.DynamicLabel.TokenExtractionRule.newBuilder()
          .setJsonPath(jsonPath)
          .setKey(key)
          .setAlias(alias)
          .build();
    } else {
      return Action.DynamicLabel.TokenExtractionRule.newBuilder()
          .setJsonPath(jsonPath)
          .setKey(key)
          .build();
    }
  }
}
