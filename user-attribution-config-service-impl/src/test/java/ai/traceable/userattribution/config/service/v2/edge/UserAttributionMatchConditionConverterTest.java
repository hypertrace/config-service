package ai.traceable.userattribution.config.service.v2.edge;

import static ai.traceable.userattribution.config.service.v2.KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS;
import static ai.traceable.userattribution.config.service.v2.ValueMatchOperator.VALUE_MATCH_OPERATOR_CONTAINS;
import static ai.traceable.userattribution.config.service.v2.ValueMatchOperator.VALUE_MATCH_OPERATOR_EQUALS;
import static ai.traceable.userattribution.config.service.v2.ValueMatchOperator.VALUE_MATCH_OPERATOR_MATCHES_REGEX;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.userattribution.config.service.v2.Attribute;
import ai.traceable.userattribution.config.service.v2.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.EnvironmentScope;
import ai.traceable.userattribution.config.service.v2.KeyMatch;
import ai.traceable.userattribution.config.service.v2.LiteralValue;
import ai.traceable.userattribution.config.service.v2.Predicate;
import ai.traceable.userattribution.config.service.v2.StringList;
import ai.traceable.userattribution.config.service.v2.UrlScope;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.jupiter.api.Test;

class UserAttributionMatchConditionConverterTest {

  @Test
  void convertEnvironment_buildsInConditionOnEnvironment() {
    EnvironmentScope scope =
        EnvironmentScope.newBuilder()
            .setEnvironmentNames(StringList.newBuilder().addValues("env1").addValues("env2"))
            .build();

    MatchCondition condition = UserAttributionMatchConditionConverter.convertEnvironment(scope);

    StructuredMatchCondition structured = condition.getStructuredMatchCondition();
    assertEquals(
        "$s.getEnvironment()",
        structured
            .getLhs()
            .getRules(0)
            .getTransformationConfig()
            .getJexlExpression()
            .getJexlExpression());

    BinaryOperator op = structured.getBinaryOperator();
    assertEquals(MatchOperator.MATCH_OPERATOR_IN, op.getMatchOperator());

    ListValue expectedList =
        ListValue.newBuilder()
            .addAllValues(
                List.of(
                    Value.newBuilder().setStringValue("env1").build(),
                    Value.newBuilder().setStringValue("env2").build()))
            .build();
    assertEquals(expectedList, op.getListValue());
  }

  @Test
  void convertUrl_buildsLikeConditionOnPathWithOrJoinedRegexes() {
    UrlScope scope =
        UrlScope.newBuilder()
            .setUrlMatchRegexes(StringList.newBuilder().addValues("^/a$").addValues("^/b$").build())
            .build();

    MatchCondition condition = UserAttributionMatchConditionConverter.convertUrl(scope);

    StructuredMatchCondition structured = condition.getStructuredMatchCondition();
    assertEquals(
        "$s.getPath()",
        structured
            .getLhs()
            .getRules(0)
            .getTransformationConfig()
            .getJexlExpression()
            .getJexlExpression());

    BinaryOperator op = structured.getBinaryOperator();
    assertEquals(MatchOperator.MATCH_OPERATOR_LIKE, op.getMatchOperator());
    assertEquals("^/a$|^/b$", op.getRegex());
  }

  @Test
  void convertPredicate_attributePredicate_mapsOperatorsAndLhsProjection() {
    AttributeProjection projection =
        AttributeProjection.newBuilder()
            .setAttribute(
                Attribute.newBuilder()
                    .setRequestHeader(
                        KeyMatch.newBuilder()
                            .setOperator(KEY_MATCH_OPERATOR_EQUALS)
                            .setMatchKey("x-user")))
            .build();

    ai.traceable.userattribution.config.service.v2.MatchCondition valueMatchCondition =
        ai.traceable.userattribution.config.service.v2.MatchCondition.newBuilder()
            .setOperator(VALUE_MATCH_OPERATOR_EQUALS)
            .setMatchValue(LiteralValue.newBuilder().setStringValue("abc"))
            .build();

    Predicate predicate =
        Predicate.newBuilder()
            .setAttributePredicate(
                Predicate.AttributePredicate.newBuilder()
                    .setAttributeProjection(projection)
                    .setAttributeValueMatchCondition(valueMatchCondition))
            .build();

    MatchCondition converted = UserAttributionMatchConditionConverter.convertPredicate(predicate);

    StructuredMatchCondition structured = converted.getStructuredMatchCondition();
    assertEquals(
        "$s.getRequestHeaders().get('x-user')",
        structured
            .getLhs()
            .getRules(0)
            .getTransformationConfig()
            .getJexlExpression()
            .getJexlExpression());
    assertEquals(
        MatchOperator.MATCH_OPERATOR_EQ, structured.getBinaryOperator().getMatchOperator());
    assertEquals("abc", structured.getBinaryOperator().getStringValue());
  }

  @Test
  void convertPredicate_logicalPredicate_convertsAndOr() {
    Predicate child1 =
        Predicate.newBuilder()
            .setAttributePredicate(
                Predicate.AttributePredicate.newBuilder()
                    .setAttributeProjection(
                        AttributeProjection.newBuilder()
                            .setAttribute(
                                Attribute.newBuilder()
                                    .setRequestHeader(
                                        KeyMatch.newBuilder()
                                            .setOperator(KEY_MATCH_OPERATOR_EQUALS)
                                            .setMatchKey("a"))))
                    .setAttributeValueMatchCondition(
                        ai.traceable.userattribution.config.service.v2.MatchCondition.newBuilder()
                            .setOperator(VALUE_MATCH_OPERATOR_CONTAINS)
                            .setMatchValue(LiteralValue.newBuilder().setStringValue("x"))))
            .build();

    Predicate child2 =
        Predicate.newBuilder()
            .setAttributePredicate(
                Predicate.AttributePredicate.newBuilder()
                    .setAttributeProjection(
                        AttributeProjection.newBuilder()
                            .setAttribute(
                                Attribute.newBuilder()
                                    .setRequestHeader(
                                        KeyMatch.newBuilder()
                                            .setOperator(KEY_MATCH_OPERATOR_EQUALS)
                                            .setMatchKey("b"))))
                    .setAttributeValueMatchCondition(
                        ai.traceable.userattribution.config.service.v2.MatchCondition.newBuilder()
                            .setOperator(VALUE_MATCH_OPERATOR_MATCHES_REGEX)
                            .setMatchValue(LiteralValue.newBuilder().setStringValue(".*y.*"))))
            .build();

    Predicate orPredicate =
        Predicate.newBuilder()
            .setLogicalPredicate(
                Predicate.LogicalPredicate.newBuilder()
                    .setOperator(Predicate.LogicalOperator.LOGICAL_OPERATOR_OR)
                    .addChildren(child1)
                    .addChildren(child2))
            .build();

    MatchCondition converted = UserAttributionMatchConditionConverter.convertPredicate(orPredicate);

    LogicalMatchCondition logical = converted.getLogicalMatchCondition();
    assertEquals(LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR, logical.getOperator());
    assertEquals(2, logical.getConditionsCount());
  }

  @Test
  void andAll_wrapsConditionsInAnd() {
    MatchCondition c1 = MatchCondition.newBuilder().build();
    MatchCondition c2 = MatchCondition.newBuilder().build();

    MatchCondition combined = UserAttributionMatchConditionConverter.andAll(List.of(c1, c2));

    assertEquals(
        LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND,
        combined.getLogicalMatchCondition().getOperator());
    assertEquals(2, combined.getLogicalMatchCondition().getConditionsCount());
  }
}
