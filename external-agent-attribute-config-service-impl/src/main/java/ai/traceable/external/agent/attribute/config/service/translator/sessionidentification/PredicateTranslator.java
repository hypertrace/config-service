package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.URL_OR_PATH_ATTRIBUTE_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalPredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ProjectorPredicate;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRuleScope;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class PredicateTranslator {
  private final AttributePredicateTranslator attributePredicateTranslator;
  private final ServiceScopeTranslator serviceScopeTranslator;
  private final AttributeRuleBuilder attributeRuleBuilder;
  private final CustomProjectionTranslator customProjectionTranslator;

  AttributeRule addConditionalPredicateIfPresent(
      List<AttributeRule.Action> actionBuilders, SessionTokenRule tokenRule) {
    if (!tokenRule.hasTokenConditionalPredicate()) {
      return buildFirstMatchingProjector(
          actionBuilders.stream()
              .map(action -> AttributeRule.newBuilder().addInitialActions(action).build())
              .collect(Collectors.toUnmodifiableList()));
    }
    return AttributeRule.newBuilder()
        .setProjector(
            addConditionalPredicate(
                buildFirstMatchingProjector(
                    actionBuilders.stream()
                        .map(action -> AttributeRule.newBuilder().addInitialActions(action).build())
                        .collect(Collectors.toUnmodifiableList())),
                tokenRule))
        .build();
  }

  AttributeRule addConditionalPredicateIfPresent(AttributeRule rule, SessionTokenRule tokenRule) {
    if (!tokenRule.hasTokenConditionalPredicate()) {
      return rule;
    }
    return AttributeRule.newBuilder()
        .setProjector(addConditionalPredicate(rule, tokenRule))
        .build();
  }

  Projector addConditionalPredicate(AttributeRule rule, SessionTokenRule tokenRule) {
    return Projector.newBuilder()
        .setConditionalProjector(
            ConditionalProjector.newBuilder()
                .setPredicate(translatePredicate(tokenRule.getTokenConditionalPredicate()))
                .setAttributeRule(rule))
        .build();
  }

  private Predicate translatePredicate(
      ai.traceable.sessionidentification.config.service.v1.Predicate predicate) {
    if (predicate.hasLogicalPredicate()) {
      return Predicate.newBuilder()
          .setLogicalPredicate(
              LogicalPredicate.newBuilder()
                  .setOperator(convert(predicate.getLogicalPredicate().getOperator()))
                  .addAllChildren(
                      predicate.getLogicalPredicate().getChildrenList().stream()
                          .map(this::translatePredicate)
                          .collect(Collectors.toUnmodifiableList())))
          .build();
    }
    if (predicate.hasCustomPredicate()) {
      return Predicate.newBuilder()
          .setProjectorPredicate(
              ProjectorPredicate.newBuilder()
                  .setProjector(
                      customProjectionTranslator.translateCustomProjection(
                          predicate.getCustomPredicate())))
          .build();
    }

    List<Projector> differentLocationPredicateProjectors =
        attributePredicateTranslator.translate(predicate.getAttributePredicate());

    if (differentLocationPredicateProjectors.size() == 1) {
      return Predicate.newBuilder()
          .setProjectorPredicate(
              ProjectorPredicate.newBuilder()
                  .setProjector(differentLocationPredicateProjectors.get(0)))
          .build();
    }

    return Predicate.newBuilder()
        .setLogicalPredicate(
            LogicalPredicate.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                .addAllChildren(
                    differentLocationPredicateProjectors.stream()
                        .map(
                            projector ->
                                Predicate.newBuilder()
                                    .setProjectorPredicate(
                                        ProjectorPredicate.newBuilder().setProjector(projector))
                                    .build())
                        .collect(Collectors.toUnmodifiableList())))
        .build();
  }

  AttributeRule addScopePredicatesIfSet(
      SessionIdentificationRuleScope scope, AttributeRule attributeRule) {
    return buildScopePredicate(scope)
        .map(
            predicate ->
                AttributeRule.newBuilder()
                    .setProjector(
                        Projector.newBuilder()
                            .setConditionalProjector(
                                ConditionalProjector.newBuilder()
                                    .setPredicate(predicate)
                                    .setAttributeRule(attributeRule)))
                    .build())
        .orElse(attributeRule);
  }

  private Optional<Predicate> buildScopePredicate(SessionIdentificationRuleScope scope) {
    Optional<Predicate> serviceScopePredicate =
        serviceScopeTranslator.addServiceScopes(
            scope.getServiceNameRegexesList(), scope.getServiceNamesList());
    if (scope.getUrlMatchRegexesCount() != 0 && serviceScopePredicate.isPresent()) {
      return Optional.of(
          Predicate.newBuilder()
              .setLogicalPredicate(
                  LogicalPredicate.newBuilder()
                      .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                      .addChildren(
                          attributeRuleBuilder.buildPredicate(
                              ComparisonOperator.COMPARISON_OPERATOR_EQUALS,
                              URL_OR_PATH_ATTRIBUTE_KEYS,
                              ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX,
                              String.join("|", scope.getUrlMatchRegexesList())))
                      .addChildren(serviceScopePredicate.get()))
              .build());
    }

    if (scope.getUrlMatchRegexesCount() != 0) {
      return Optional.of(
          attributeRuleBuilder.buildPredicate(
              ComparisonOperator.COMPARISON_OPERATOR_EQUALS,
              URL_OR_PATH_ATTRIBUTE_KEYS,
              ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX,
              String.join("|", scope.getUrlMatchRegexesList())));
    }

    return serviceScopePredicate;
  }

  AttributeRule buildFirstMatchingProjector(List<AttributeRule> rules) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setFirstMatchingProjector(
                    Projector.FirstMatchingProjector.newBuilder().addAllAttributeRules(rules)))
        .build();
  }

  LogicalOperator convert(
      ai.traceable.sessionidentification.config.service.v1.Predicate.LogicalOperator operator) {
    switch (operator) {
      case LOGICAL_OPERATOR_AND:
        return LogicalOperator.LOGICAL_OPERATOR_AND;
      case LOGICAL_OPERATOR_OR:
        return LogicalOperator.LOGICAL_OPERATOR_OR;
      default:
        return LogicalOperator.LOGICAL_OPERATOR_UNSPECIFIED;
    }
  }
}
