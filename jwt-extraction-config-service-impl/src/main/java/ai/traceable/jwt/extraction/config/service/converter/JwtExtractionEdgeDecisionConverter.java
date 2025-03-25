package ai.traceable.jwt.extraction.config.service.converter;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_STR;
import static ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND;
import static ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_EQ;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_LIKE;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_NOT_EQ;
import static ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_NOT_LIKE;
import static ai.traceable.edge.decision.config.service.v1.EdgeInputKind.EDGE_INPUT_KIND_HTTP_REQUEST;

import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.UnaryOperator;
import ai.traceable.edge.decision.config.service.v1.EdgeAttributionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeAttributionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import ai.traceable.edge.decision.config.service.v1.SpanAttributeDecoration;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRuleScope;
import ai.traceable.jwt.extraction.config.service.v1.Predicate;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JwtExtractionEdgeDecisionConverter {
  private static final StructuredMatchCondition CONDITION_WITH_PATH =
      StructuredMatchCondition.newBuilder()
          .setLhs(
              AttributeDerivationMapping.newBuilder()
                  .setName("lhs")
                  .setType(FIELD_TYPE_STR)
                  .addRules(
                      DerivationRule.newBuilder()
                          .setTransformationConfig(
                              DataTransformationConfig.newBuilder()
                                  .setOutputType(FIELD_TYPE_STR)
                                  .setJexlExpression(
                                      JexlExpressionConfig.newBuilder()
                                          .setJexlExpression("$s.getPath()")))))
          .build();
  private static final StructuredMatchCondition CONDITION_WITH_PATH_NOT_EMPTY =
      CONDITION_WITH_PATH.toBuilder()
          .setUnaryOperator(UnaryOperator.UNARY_OPERATOR_IS_NOT_EMPTY)
          .build();
  private static final MatchCondition MATCH_CONDITION_WITH_PATH_NOT_EMPTY =
      MatchCondition.newBuilder()
          .setStructuredMatchCondition(CONDITION_WITH_PATH_NOT_EMPTY)
          .build();

  public EdgeDecisionEngineConfig convert(final List<JwtExtractionRule> jwtExtractionRules) {
    final EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    jwtExtractionRules.stream()
        .map(this::convertJwtExtractionRule)
        .flatMap(Optional::stream)
        .forEach(builder::addAttributionRules);
    return builder.build();
  }

  private Optional<EdgeAttributionRule> convertJwtExtractionRule(
      final JwtExtractionRule jwtExtractionRule) {
    try {
      final EdgeAttributionRule.Builder builder = EdgeAttributionRule.newBuilder();
      builder.setId(jwtExtractionRule.getId());
      builder.setName(jwtExtractionRule.getName());
      builder.setRuleStatus(buildRuleStatus(jwtExtractionRule));
      buildRuleScope(jwtExtractionRule).ifPresent(builder::setRuleScope);
      builder.setRuleDefinition(buildRuleDefinition(jwtExtractionRule));
      return Optional.of(builder.build());
    } catch (Exception ex) {
      log.warn("Unable to convert jwt extraction rule: {}", jwtExtractionRule, ex);
      return Optional.empty();
    }
  }

  private EdgeDecisionRuleStatus.Builder buildRuleStatus(
      final JwtExtractionRule jwtExtractionRule) {
    return EdgeDecisionRuleStatus.newBuilder().setDisabled(jwtExtractionRule.getDisabled());
  }

  private Optional<EdgeDecisionRuleScope> buildRuleScope(
      final JwtExtractionRule jwtExtractionRule) {
    final JwtExtractionRuleScope scope = jwtExtractionRule.getScope();
    final EdgeDecisionRuleScope.Builder builder = EdgeDecisionRuleScope.newBuilder();
    if (scope.hasEnvironmentScope()
        && !scope.getEnvironmentScope().getEnvironmentNamesList().isEmpty()) {
      builder.addScopeConditions(
          EdgeDecisionRuleScopeCondition.newBuilder()
              .setEnvironmentScope(
                  EnvironmentScope.newBuilder()
                      .addAllEnvironments(scope.getEnvironmentScope().getEnvironmentNamesList())));
    }
    return builder.getScopeConditionsList().isEmpty()
        ? Optional.empty()
        : Optional.of(builder.build());
  }

  private List<SpanAttributeDecoration> buildSpanAttributeDecorations(
      JwtExtractionRule jwtExtractionRule) {
    return new AttributeProjection(jwtExtractionRule).getSpanAttributeDecoration();
  }

  private EdgeAttributionRuleDefinition.Builder buildRuleDefinition(
      JwtExtractionRule jwtExtractionRule) {
    return EdgeAttributionRuleDefinition.newBuilder()
        .setEdgeInputKind(EDGE_INPUT_KIND_HTTP_REQUEST)
        .setMatchCondition(buildMatchCondition(jwtExtractionRule.getPredicate()))
        .addAllSpanAttributes(buildSpanAttributeDecorations(jwtExtractionRule));
  }

  private MatchCondition buildMatchCondition(Predicate predicate) {
    switch (predicate.getPredicateCase()) {
      case URL_PREDICATE:
        return MatchCondition.newBuilder()
            .setStructuredMatchCondition(buildStructuredMatchCondition(predicate.getUrlPredicate()))
            .build();
      case COMPOSITE_PREDICATE:
        LogicalMatchCondition.Builder logicalMatchConditionBuilder =
            LogicalMatchCondition.newBuilder();
        Predicate.CompositePredicate compositePredicate = predicate.getCompositePredicate();
        logicalMatchConditionBuilder.setOperator(convertOperator(compositePredicate.getOperator()));
        List<MatchCondition> childMatchConditions =
            compositePredicate.getChildrenList().stream()
                .map(this::buildMatchCondition)
                .collect(Collectors.toUnmodifiableList());
        logicalMatchConditionBuilder.addAllConditions(childMatchConditions);
        return MatchCondition.newBuilder()
            .setLogicalMatchCondition(logicalMatchConditionBuilder)
            .build();
      default:
        return MATCH_CONDITION_WITH_PATH_NOT_EMPTY;
    }
  }

  private StructuredMatchCondition.Builder buildStructuredMatchCondition(
      StringPredicate urlPredicate) {
    BinaryOperator.Builder builder = BinaryOperator.newBuilder();
    switch (urlPredicate.getOperator()) {
      case RELATIONAL_OPERATOR_EQUALS:
        builder.setMatchOperator(MATCH_OPERATOR_EQ);
        builder.setStringValue(urlPredicate.getValue());
        break;
      case RELATIONAL_OPERATOR_NOT_EQUALS:
        builder.setMatchOperator(MATCH_OPERATOR_NOT_EQ);
        builder.setStringValue(urlPredicate.getValue());
        break;
      case RELATIONAL_OPERATOR_MATCHES_REGEX:
        builder.setMatchOperator(MATCH_OPERATOR_LIKE);
        builder.setRegex(urlPredicate.getValue());
        break;
      case RELATIONAL_OPERATOR_NOT_MATCHES_REGEX:
        builder.setMatchOperator(MATCH_OPERATOR_NOT_LIKE);
        builder.setRegex(urlPredicate.getValue());
        break;
      default:
        throw new IllegalArgumentException("operator not supported: " + urlPredicate.getOperator());
    }
    return CONDITION_WITH_PATH.toBuilder().setBinaryOperator(builder);
  }

  private LogicalMatchOperator convertOperator(
      Predicate.CompositePredicate.LogicalOperator operator) {
    switch (operator) {
      case LOGICAL_OPERATOR_AND:
        return LOGICAL_MATCH_OPERATOR_AND;
      case LOGICAL_OPERATOR_OR:
        return LOGICAL_MATCH_OPERATOR_OR;
      default:
        throw new IllegalArgumentException("unknown operator: " + operator);
    }
  }
}
