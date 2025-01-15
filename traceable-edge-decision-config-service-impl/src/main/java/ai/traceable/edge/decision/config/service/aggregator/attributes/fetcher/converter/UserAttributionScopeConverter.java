package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND;
import static ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_EQ;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_LIKE;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_NOT_EQ;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_STARTS_WITH;
import static ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.UserAttributionJexlGenerator.JEXL_BASE_EXPRESSION;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.EnvironmentScope;
import ai.traceable.userattribution.config.service.v2.Predicate;
import ai.traceable.userattribution.config.service.v2.Predicate.AttributePredicate;
import ai.traceable.userattribution.config.service.v2.Predicate.LogicalPredicate;
import ai.traceable.userattribution.config.service.v2.UrlScope;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleScope;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class UserAttributionScopeConverter {
  private static final AttributeDerivationMapping ENVIRONMENT_DERIVATION_RULE =
      createAttributeDerivationMapping(JEXL_BASE_EXPRESSION, ".getEnvironment()");
  private static final AttributeDerivationMapping PATH_DERIVATION_RULE =
      createAttributeDerivationMapping(JEXL_BASE_EXPRESSION, ".getPath()");

  public Optional<MatchCondition> convert(UserAttributionRuleData ruleData) {
    List<MatchCondition> conditions = new ArrayList<>();
    if (ruleData.hasScope()) {
      UserAttributionRuleScope scope = ruleData.getScope();
      if (scope.hasEnvironmentScope()) {
        conditions.add(convert(scope.getEnvironmentScope()));
      }
      if (scope.hasServiceScope()) {
        throw new IllegalArgumentException("service scope is not supported");
      }
      if (scope.hasUrlScope()) {
        conditions.add(convert(scope.getUrlScope()));
      }
    }
    UserAttributionRootTokenRule rootTokenRule = ruleData.getRootTokenRule();
    UserAttributionTokenRule userIdRule = ruleData.getUserIdRule();
    if (userIdRule.hasRootRelativeProjection() && rootTokenRule.hasTokenConditionalPredicate()) {
      conditions.add(convert(rootTokenRule.getTokenConditionalPredicate()));
    }
    if (userIdRule.hasTokenConditionalPredicate()) {
      conditions.add(convert(userIdRule.getTokenConditionalPredicate()));
    }
    if (conditions.isEmpty()) {
      return Optional.empty();
    }
    if (conditions.size() == 1) {
      return Optional.of(conditions.get(0));
    } else {
      return Optional.of(
          MatchCondition.newBuilder()
              .setLogicalMatchCondition(
                  LogicalMatchCondition.newBuilder()
                      .setOperator(LOGICAL_MATCH_OPERATOR_AND)
                      .addAllConditions(conditions))
              .build());
    }
  }

  private MatchCondition convert(EnvironmentScope environmentScope) {
    StructuredMatchCondition.Builder builder = StructuredMatchCondition.newBuilder();
    builder.setLhs(ENVIRONMENT_DERIVATION_RULE);
    builder.setBinaryOperator(
        BinaryOperator.newBuilder()
            .setMatchOperator(MatchOperator.MATCH_OPERATOR_IN)
            .setListValue(
                ListValue.newBuilder()
                    .addAllValues(
                        environmentScope.getEnvironmentNames().getValuesList().stream()
                            .map(
                                environmentName ->
                                    Value.newBuilder().setStringValue(environmentName).build())
                            .collect(Collectors.toUnmodifiableList()))));
    return MatchCondition.newBuilder().setStructuredMatchCondition(builder).build();
  }

  private MatchCondition convert(UrlScope urlScope) {
    StructuredMatchCondition.Builder builder = StructuredMatchCondition.newBuilder();
    builder.setLhs(PATH_DERIVATION_RULE);
    builder.setBinaryOperator(
        BinaryOperator.newBuilder()
            .setMatchOperator(MatchOperator.MATCH_OPERATOR_LIKE)
            .setRegex(String.join("|", urlScope.getUrlMatchRegexes().getValuesList())));
    return MatchCondition.newBuilder().setStructuredMatchCondition(builder).build();
  }

  private MatchCondition convert(Predicate tokenConditionalPredicate) {
    switch (tokenConditionalPredicate.getPredicateCase()) {
      case ATTRIBUTE_PREDICATE:
        return convert(tokenConditionalPredicate.getAttributePredicate());
      case LOGICAL_PREDICATE:
        return convert(tokenConditionalPredicate.getLogicalPredicate());
      default:
        throw new IllegalArgumentException(
            "unknown predicate case: " + tokenConditionalPredicate.getPredicateCase());
    }
  }

  private MatchCondition convert(AttributePredicate attributePredicate) {
    String attributeProjectionStr =
        new AttributeProjection(attributePredicate.getAttributeProjection())
            .apply(JEXL_BASE_EXPRESSION);
    StructuredMatchCondition.Builder builder = StructuredMatchCondition.newBuilder();
    builder.setLhs(
        AttributeDerivationMapping.newBuilder()
            .setName("lhs")
            .addRules(
                DerivationRule.newBuilder()
                    .setTransformationConfig(
                        DataTransformationConfig.newBuilder()
                            .setJexlExpression(
                                JexlExpressionConfig.newBuilder()
                                    .setJexlExpression(attributeProjectionStr))
                            .setOutputType(FIELD_TYPE_STR))));
    builder.setBinaryOperator(convert(attributePredicate.getAttributeValueMatchCondition()));
    return MatchCondition.newBuilder().setStructuredMatchCondition(builder).build();
  }

  private MatchCondition convert(LogicalPredicate logicalPredicate) {
    List<MatchCondition> childConditions =
        logicalPredicate.getChildrenList().stream()
            .map(this::convert)
            .collect(Collectors.toUnmodifiableList());
    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .addAllConditions(childConditions)
                .setOperator(convert(logicalPredicate.getOperator())))
        .build();
  }

  private LogicalMatchOperator convert(Predicate.LogicalOperator operator) {
    switch (operator) {
      case LOGICAL_OPERATOR_OR:
        return LOGICAL_MATCH_OPERATOR_OR;
      case LOGICAL_OPERATOR_AND:
        return LOGICAL_MATCH_OPERATOR_AND;
      default:
        throw new IllegalArgumentException("unknown logical operator case: " + operator);
    }
  }

  private BinaryOperator convert(
      ai.traceable.userattribution.config.service.v2.MatchCondition attributeValueMatchCondition) {
    switch (attributeValueMatchCondition.getOperator()) {
      case VALUE_MATCH_OPERATOR_EQUALS:
        return BinaryOperator.newBuilder()
            .setStringValue(attributeValueMatchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_EQ)
            .build();
      case VALUE_MATCH_OPERATOR_NOT_EQUALS:
        return BinaryOperator.newBuilder()
            .setStringValue(attributeValueMatchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_NOT_EQ)
            .build();
      case VALUE_MATCH_OPERATOR_CONTAINS:
        return BinaryOperator.newBuilder()
            .setStringValue(attributeValueMatchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_CONTAINS)
            .build();
      case VALUE_MATCH_OPERATOR_STARTS_WITH:
        return BinaryOperator.newBuilder()
            .setStringValue(attributeValueMatchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_STARTS_WITH)
            .build();
      case VALUE_MATCH_OPERATOR_MATCHES_REGEX:
        return BinaryOperator.newBuilder()
            .setRegex(attributeValueMatchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_LIKE)
            .build();
      default:
        throw new IllegalArgumentException(
            "unknown match operator case: " + attributeValueMatchCondition.getOperator());
    }
  }

  private static AttributeDerivationMapping createAttributeDerivationMapping(
      String inputExpression, String methodSuffix) {
    return AttributeDerivationMapping.newBuilder()
        .setType(FIELD_TYPE_STR)
        .setName("lhs")
        .addRules(
            DerivationRule.newBuilder()
                .setTransformationConfig(
                    DataTransformationConfig.newBuilder()
                        .setJexlExpression(
                            JexlExpressionConfig.newBuilder()
                                .setJexlExpression(inputExpression + methodSuffix))
                        .setOutputType(FIELD_TYPE_STR)))
        .build();
  }
}
