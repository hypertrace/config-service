package org.hypertrace.config.span.processing.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.LogicalOperator;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanFilterMatcherTest {

  private SpanFilterMatcher spanFilterMatcher;

  @BeforeEach
  void setup() {
    this.spanFilterMatcher = new SpanFilterMatcher();
  }

  @Test
  void testMatcher() {
    assertTrue(
        this.spanFilterMatcher.matches(
            "name", buildSpanFilterValue("nam"), RelationalOperator.RELATIONAL_OPERATOR_CONTAINS));
    assertFalse(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue("names"),
            RelationalOperator.RELATIONAL_OPERATOR_CONTAINS));

    assertTrue(
        this.spanFilterMatcher.matches(
            "name", buildSpanFilterValue("name"), RelationalOperator.RELATIONAL_OPERATOR_EQUALS));
    assertFalse(
        this.spanFilterMatcher.matches(
            "name", buildSpanFilterValue("names"), RelationalOperator.RELATIONAL_OPERATOR_EQUALS));

    assertTrue(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue("names"),
            RelationalOperator.RELATIONAL_OPERATOR_NOT_EQUALS));
    assertFalse(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue("name"),
            RelationalOperator.RELATIONAL_OPERATOR_NOT_EQUALS));

    assertTrue(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue("nam"),
            RelationalOperator.RELATIONAL_OPERATOR_STARTS_WITH));
    assertFalse(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue("names"),
            RelationalOperator.RELATIONAL_OPERATOR_STARTS_WITH));

    assertTrue(
        this.spanFilterMatcher.matches(
            "name", buildSpanFilterValue("me"), RelationalOperator.RELATIONAL_OPERATOR_ENDS_WITH));
    assertFalse(
        this.spanFilterMatcher.matches(
            "name", buildSpanFilterValue("s"), RelationalOperator.RELATIONAL_OPERATOR_ENDS_WITH));

    assertTrue(
        this.spanFilterMatcher.matches(
            "name12",
            buildSpanFilterValue("[a-zA-z]+[0-9]+"),
            RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH));
    // verify partial regex match
    assertTrue(
        this.spanFilterMatcher.matches(
            "name12",
            buildSpanFilterValue("[a-zA-z]+"),
            RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH));
    assertTrue(
        this.spanFilterMatcher.matches(
            "name12",
            buildSpanFilterValue("[a-zA-z]+[0-9]+"),
            RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH));
    assertFalse(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue("[a-zA-z]+[0-9]+"),
            RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH));
    assertFalse(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue("[(name"),
            RelationalOperator.RELATIONAL_OPERATOR_REGEX_MATCH));

    assertTrue(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue(List.of("name", "first", "last")),
            RelationalOperator.RELATIONAL_OPERATOR_IN));
    assertFalse(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue(List.of("first", "last")),
            RelationalOperator.RELATIONAL_OPERATOR_IN));

    assertFalse(
        this.spanFilterMatcher.matches(
            "name", buildSpanFilterValue("nam"), RelationalOperator.RELATIONAL_OPERATOR_IN));
    assertFalse(
        this.spanFilterMatcher.matches(
            "name",
            buildSpanFilterValue(List.of("name", "name1")),
            RelationalOperator.RELATIONAL_OPERATOR_CONTAINS));
  }

  @Test
  void serviceNameMatcherTest() {
    assertTrue(
        this.spanFilterMatcher.matchesServiceName(
            buildSpanFilter(
                buildRelationalSpanFilter(
                    Field.FIELD_SERVICE_NAME,
                    RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                    buildSpanFilterValue("serviceName"))),
            "serviceName"));

    assertFalse(
        this.spanFilterMatcher.matchesServiceName(
            buildSpanFilter(
                buildRelationalSpanFilter(
                    Field.FIELD_SERVICE_NAME,
                    RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                    buildSpanFilterValue("serviceName1"))),
            "serviceName"));

    assertTrue(
        this.spanFilterMatcher.matchesServiceName(
            buildSpanFilter(
                LogicalOperator.LOGICAL_OPERATOR_AND,
                List.of(
                    buildRelationalSpanFilter(
                        Field.FIELD_SERVICE_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                        buildSpanFilterValue("service")),
                    buildRelationalSpanFilter(
                        Field.FIELD_ENVIRONMENT_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                        buildSpanFilterValue("env1")),
                    buildRelationalSpanFilter(
                        Field.FIELD_SERVICE_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_STARTS_WITH,
                        buildSpanFilterValue("ser")))),
            "serviceName"));

    assertTrue(
        this.spanFilterMatcher.matchesServiceName(
            buildSpanFilter(
                LogicalOperator.LOGICAL_OPERATOR_OR,
                List.of(
                    buildRelationalSpanFilter(
                        Field.FIELD_SERVICE_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                        buildSpanFilterValue("service")),
                    buildRelationalSpanFilter(
                        Field.FIELD_SERVICE_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_STARTS_WITH,
                        buildSpanFilterValue("ice")))),
            "serviceName"));

    assertTrue(
        this.spanFilterMatcher.matchesServiceName(
            buildSpanFilter(
                LogicalOperator.LOGICAL_OPERATOR_AND,
                List.of(
                    buildRelationalSpanFilter(
                        Field.FIELD_SERVICE_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                        buildSpanFilterValue("service")))),
            "serviceName"));

    assertTrue(
        this.spanFilterMatcher.matchesServiceName(
            buildSpanFilter(
                LogicalOperator.LOGICAL_OPERATOR_OR,
                List.of(
                    buildRelationalSpanFilter(
                        Field.FIELD_SERVICE_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                        buildSpanFilterValue("service")))),
            "serviceName"));

    assertTrue(
        this.spanFilterMatcher.matchesServiceName(
            buildSpanFilter(
                LogicalOperator.LOGICAL_OPERATOR_AND,
                List.of(
                    buildRelationalSpanFilter(
                        Field.FIELD_SERVICE_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                        buildSpanFilterValue("service")))),
            "serviceName"));

    assertTrue(
        this.spanFilterMatcher.matchesServiceName(
            buildSpanFilter(LogicalOperator.LOGICAL_OPERATOR_AND, List.of()), "serviceName"));

    assertTrue(
        this.spanFilterMatcher.matchesServiceName(
            buildSpanFilter(LogicalOperator.LOGICAL_OPERATOR_OR, List.of()), "serviceName"));
  }

  @Test
  void environmentNameMatcherTest() {
    assertTrue(
        this.spanFilterMatcher.matchesEnvironment(
            buildSpanFilter(
                buildRelationalSpanFilter(
                    Field.FIELD_ENVIRONMENT_NAME,
                    RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                    buildSpanFilterValue("env"))),
            Optional.of("env")));

    assertFalse(
        this.spanFilterMatcher.matchesEnvironment(
            buildSpanFilter(
                buildRelationalSpanFilter(
                    Field.FIELD_ENVIRONMENT_NAME,
                    RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                    buildSpanFilterValue("env1"))),
            Optional.of("env")));

    assertTrue(
        this.spanFilterMatcher.matchesEnvironment(
            buildSpanFilter(
                buildRelationalSpanFilter(
                    Field.FIELD_ENVIRONMENT_NAME,
                    RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                    buildSpanFilterValue("env1"))),
            Optional.empty()));

    assertTrue(
        this.spanFilterMatcher.matchesEnvironment(
            buildSpanFilter(
                LogicalOperator.LOGICAL_OPERATOR_AND,
                List.of(
                    buildRelationalSpanFilter(
                        Field.FIELD_ENVIRONMENT_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                        buildSpanFilterValue("en")),
                    buildRelationalSpanFilter(
                        Field.FIELD_SERVICE_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_EQUALS,
                        buildSpanFilterValue("service1")),
                    buildRelationalSpanFilter(
                        Field.FIELD_ENVIRONMENT_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_STARTS_WITH,
                        buildSpanFilterValue("e")))),
            Optional.of("env")));

    assertTrue(
        this.spanFilterMatcher.matchesEnvironment(
            buildSpanFilter(
                LogicalOperator.LOGICAL_OPERATOR_OR,
                List.of(
                    buildRelationalSpanFilter(
                        Field.FIELD_ENVIRONMENT_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                        buildSpanFilterValue("env")),
                    buildRelationalSpanFilter(
                        Field.FIELD_ENVIRONMENT_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_STARTS_WITH,
                        buildSpanFilterValue("ice")))),
            Optional.of("env")));

    assertTrue(
        this.spanFilterMatcher.matchesEnvironment(
            buildSpanFilter(
                LogicalOperator.LOGICAL_OPERATOR_AND,
                List.of(
                    buildRelationalSpanFilter(
                        Field.FIELD_ENVIRONMENT_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                        buildSpanFilterValue("en")))),
            Optional.of("env")));

    assertTrue(
        this.spanFilterMatcher.matchesEnvironment(
            buildSpanFilter(
                LogicalOperator.LOGICAL_OPERATOR_OR,
                List.of(
                    buildRelationalSpanFilter(
                        Field.FIELD_ENVIRONMENT_NAME,
                        RelationalOperator.RELATIONAL_OPERATOR_CONTAINS,
                        buildSpanFilterValue("env")))),
            Optional.of("env")));

    assertTrue(
        this.spanFilterMatcher.matchesEnvironment(
            buildSpanFilter(LogicalOperator.LOGICAL_OPERATOR_AND, List.of()), Optional.of("env")));

    assertTrue(
        this.spanFilterMatcher.matchesEnvironment(
            buildSpanFilter(LogicalOperator.LOGICAL_OPERATOR_OR, List.of()), Optional.of("env")));
  }

  private SpanFilter buildSpanFilter(
      RelationalSpanFilterExpression relationalSpanFilterExpression) {
    return SpanFilter.newBuilder().setRelationalSpanFilter(relationalSpanFilterExpression).build();
  }

  private SpanFilter buildSpanFilter(
      LogicalOperator operator,
      List<RelationalSpanFilterExpression> relationalSpanFilterExpressions) {
    return SpanFilter.newBuilder()
        .setLogicalSpanFilter(
            LogicalSpanFilterExpression.newBuilder()
                .setOperator(operator)
                .addAllOperands(
                    relationalSpanFilterExpressions.stream()
                        .map(
                            relationalSpanFilterExpression ->
                                SpanFilter.newBuilder()
                                    .setRelationalSpanFilter(relationalSpanFilterExpression)
                                    .build())
                        .collect(Collectors.toUnmodifiableList()))
                .build())
        .build();
  }

  private RelationalSpanFilterExpression buildRelationalSpanFilter(
      Field field, RelationalOperator operator, SpanFilterValue spanFilterValue) {
    return RelationalSpanFilterExpression.newBuilder()
        .setField(field)
        .setOperator(operator)
        .setRightOperand(spanFilterValue)
        .build();
  }

  private SpanFilterValue buildSpanFilterValue(List<String> rhs) {
    return SpanFilterValue.newBuilder()
        .setListValue(
            org.hypertrace.span.processing.config.service.v1.ListValue.newBuilder()
                .addAllValues(
                    rhs.stream()
                        .map(val -> SpanFilterValue.newBuilder().setStringValue(val).build())
                        .collect(Collectors.toUnmodifiableList()))
                .build())
        .build();
  }

  private SpanFilterValue buildSpanFilterValue(String rhs) {
    return SpanFilterValue.newBuilder().setStringValue(rhs).build();
  }
}
