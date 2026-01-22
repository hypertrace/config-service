package ai.traceable.userattribution.config.service.v2.edge;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND;
import static ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_EQ;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_LIKE;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_NOT_EQ;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_STARTS_WITH;

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
import ai.traceable.userattribution.config.service.v2.EnvironmentScope;
import ai.traceable.userattribution.config.service.v2.Predicate;
import ai.traceable.userattribution.config.service.v2.UrlScope;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import java.util.List;
import java.util.stream.Collectors;

final class UserAttributionMatchConditionConverter {
  private static final AttributeDerivationMapping ENVIRONMENT_DERIVATION_RULE =
      createAttributeDerivationMapping("$s", ".getEnvironment()");
  private static final AttributeDerivationMapping PATH_DERIVATION_RULE =
      createAttributeDerivationMapping("$s", ".getPath()");

  private UserAttributionMatchConditionConverter() {}

  static MatchCondition andAll(List<MatchCondition> conditions) {
    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .setOperator(LOGICAL_MATCH_OPERATOR_AND)
                .addAllConditions(conditions))
        .build();
  }

  static MatchCondition convertEnvironment(EnvironmentScope environmentScope) {
    if (environmentScope.getEnvironmentScopeCase()
        != EnvironmentScope.EnvironmentScopeCase.ENVIRONMENT_NAMES) {
      throw new IllegalArgumentException("Environment scope is not specified correctly");
    }
    StructuredMatchCondition.Builder builder = StructuredMatchCondition.newBuilder();
    builder.setLhs(ENVIRONMENT_DERIVATION_RULE);
    builder.setBinaryOperator(
        BinaryOperator.newBuilder()
            .setMatchOperator(MatchOperator.MATCH_OPERATOR_IN)
            .setListValue(
                ListValue.newBuilder()
                    .addAllValues(
                        environmentScope.getEnvironmentNames().getValuesList().stream()
                            .map(name -> Value.newBuilder().setStringValue(name).build())
                            .collect(Collectors.toUnmodifiableList()))));
    return MatchCondition.newBuilder().setStructuredMatchCondition(builder).build();
  }

  static MatchCondition convertUrl(UrlScope urlScope) {
    if (urlScope.getUrlScopeCase() != UrlScope.UrlScopeCase.URL_MATCH_REGEXES) {
      throw new IllegalArgumentException("Url scope is not specified correctly");
    }
    StructuredMatchCondition.Builder builder = StructuredMatchCondition.newBuilder();
    builder.setLhs(PATH_DERIVATION_RULE);
    builder.setBinaryOperator(
        BinaryOperator.newBuilder()
            .setMatchOperator(MatchOperator.MATCH_OPERATOR_LIKE)
            .setRegex(String.join("|", urlScope.getUrlMatchRegexes().getValuesList())));
    return MatchCondition.newBuilder().setStructuredMatchCondition(builder).build();
  }

  static MatchCondition convertPredicate(Predicate predicate) {
    switch (predicate.getPredicateCase()) {
      case ATTRIBUTE_PREDICATE:
        return convertAttributePredicate(predicate.getAttributePredicate());
      case LOGICAL_PREDICATE:
        return convertLogicalPredicate(predicate.getLogicalPredicate());
      default:
        throw new IllegalArgumentException(
            "unknown predicate case: " + predicate.getPredicateCase());
    }
  }

  private static MatchCondition convertAttributePredicate(
      Predicate.AttributePredicate attributePredicate) {
    String attributeProjectionStr =
        UserAttributionJexlProjectionUtils.applyAttributeProjection(
            "$s", attributePredicate.getAttributeProjection());

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
    builder.setBinaryOperator(
        convertMatchCondition(attributePredicate.getAttributeValueMatchCondition()));
    return MatchCondition.newBuilder().setStructuredMatchCondition(builder).build();
  }

  private static MatchCondition convertLogicalPredicate(
      Predicate.LogicalPredicate logicalPredicate) {
    List<MatchCondition> childConditions =
        logicalPredicate.getChildrenList().stream()
            .map(UserAttributionMatchConditionConverter::convertPredicate)
            .collect(Collectors.toUnmodifiableList());

    return MatchCondition.newBuilder()
        .setLogicalMatchCondition(
            LogicalMatchCondition.newBuilder()
                .addAllConditions(childConditions)
                .setOperator(convertOperator(logicalPredicate.getOperator())))
        .build();
  }

  private static LogicalMatchOperator convertOperator(Predicate.LogicalOperator operator) {
    switch (operator) {
      case LOGICAL_OPERATOR_OR:
        return LOGICAL_MATCH_OPERATOR_OR;
      case LOGICAL_OPERATOR_AND:
        return LOGICAL_MATCH_OPERATOR_AND;
      default:
        throw new IllegalArgumentException("unknown logical operator case: " + operator);
    }
  }

  private static BinaryOperator convertMatchCondition(
      ai.traceable.userattribution.config.service.v2.MatchCondition matchCondition) {
    switch (matchCondition.getOperator()) {
      case VALUE_MATCH_OPERATOR_EQUALS:
        return BinaryOperator.newBuilder()
            .setStringValue(matchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_EQ)
            .build();
      case VALUE_MATCH_OPERATOR_NOT_EQUALS:
        return BinaryOperator.newBuilder()
            .setStringValue(matchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_NOT_EQ)
            .build();
      case VALUE_MATCH_OPERATOR_CONTAINS:
        return BinaryOperator.newBuilder()
            .setStringValue(matchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_CONTAINS)
            .build();
      case VALUE_MATCH_OPERATOR_STARTS_WITH:
        return BinaryOperator.newBuilder()
            .setStringValue(matchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_STARTS_WITH)
            .build();
      case VALUE_MATCH_OPERATOR_MATCHES_REGEX:
        return BinaryOperator.newBuilder()
            .setRegex(matchCondition.getMatchValue().getStringValue())
            .setMatchOperator(MATCH_OPERATOR_LIKE)
            .build();
      default:
        throw new IllegalArgumentException(
            "unknown match operator case: " + matchCondition.getOperator());
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
