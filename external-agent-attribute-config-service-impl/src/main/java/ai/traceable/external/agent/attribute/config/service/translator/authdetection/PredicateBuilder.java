package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.auth.detection.config.service.v1.Predicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.CompositePredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.KeyValuePredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate.RelationalOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.AttributePredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.Builder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalPredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.StringPredicate;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
class PredicateBuilder {
  private final Provider<AuthDetectionRuleTranslatorLookup> translatorLookupProvider;

  Optional<ConditionalProjector.Predicate> translateCompositePredicate(
      CompositePredicate compositePredicate) {
    List<ConditionalProjector.Predicate> children =
        compositePredicate.getChildrenList().stream()
            .map(
                childPredicate ->
                    this.translatorLookupProvider
                        .get()
                        .getRuleTranslator(childPredicate)
                        .flatMap(translator -> translator.translatePredicate(childPredicate)))
            .flatMap(Optional::stream)
            .collect(toUnmodifiableList());

    // If we fail to translate any child predicate - as indicated by a size mismatch - drop the
    // whole thing (as it may not be semantically correct)
    if (children.size() != compositePredicate.getChildrenCount()) {
      log.error(
          "Composite predicate contains one or more unsupported children, omitting entire predicate: {}",
          compositePredicate);
      return Optional.empty();
    }

    return this.translateLogicalOperator(compositePredicate.getOperator())
        .map(
            translatedOperator ->
                ConditionalProjector.Predicate.newBuilder()
                    .setLogicalPredicate(
                        LogicalPredicate.newBuilder()
                            .setOperator(translatedOperator)
                            .addAllChildren(children))
                    .build());
  }

  private Optional<ComparisonOperator> translateRelationalOperator(
      RelationalOperator relationalOperator) {
    switch (relationalOperator) {
      case RELATIONAL_OPERATOR_EQUALS:
        return Optional.of(ComparisonOperator.COMPARISON_OPERATOR_EQUALS);
      case RELATIONAL_OPERATOR_MATCHES_REGEX:
        return Optional.of(ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX);
      case RELATIONAL_OPERATOR_NOT_EQUALS:
      case RELATIONAL_OPERATOR_NOT_MATCHES_REGEX:
        // TODO not equals, not matches regex
      case UNRECOGNIZED:
      case RELATIONAL_OPERATOR_UNSPECIFIED:
      default:
        log.error("Received operator {} not supported", relationalOperator);
        return Optional.empty();
    }
  }

  private Optional<LogicalOperator> translateLogicalOperator(
      CompositePredicate.LogicalOperator logicalOperator) {
    switch (logicalOperator) {
      case LOGICAL_OPERATOR_AND:
        return Optional.of(LogicalOperator.LOGICAL_OPERATOR_AND);
      case LOGICAL_OPERATOR_OR:
        return Optional.of(LogicalOperator.LOGICAL_OPERATOR_OR);
      case LOGICAL_OPERATOR_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        log.error("Received operator {} not supported", logicalOperator);
        return Optional.empty();
    }
  }

  private Optional<StringPredicate> translateStringPredicate(
      Predicate.StringPredicate stringPredicate) {
    return this.translateRelationalOperator(stringPredicate.getOperator())
        .map(
            translatedOperator ->
                StringPredicate.newBuilder()
                    .setOperator(translatedOperator)
                    .setValue(stringPredicate.getValue())
                    .build());
  }

  Optional<ConditionalProjector.Predicate> translateAttributeKeyPredicate(
      Predicate.StringPredicate stringPredicate) {
    return this.translateStringPredicate(stringPredicate)
        .map(
            translatedStringPredicate ->
                AttributePredicate.newBuilder().setNamePredicate(translatedStringPredicate))
        .map(
            attributePredicate ->
                ConditionalProjector.Predicate.newBuilder()
                    .setAttributePredicate(attributePredicate))
        .map(Builder::build);
  }

  Optional<ConditionalProjector.Predicate> translateCurrentValuePredicate(
      Predicate.StringPredicate stringPredicate) {
    return this.translateStringPredicate(stringPredicate)
        .map(
            translatedStringPredicate ->
                ConditionalProjector.Predicate.newBuilder()
                    .setCurrentValuePredicate(translatedStringPredicate))
        .map(Builder::build);
  }

  Optional<ConditionalProjector.Predicate> translateAttributeKeyValuePredicate(
      KeyValuePredicate keyValuePredicate) {

    if (!keyValuePredicate.hasValuePredicate()) {
      // TODO - verify agent is correctly handling optional presence on value predicate
      return this.translateAttributeKeyPredicate(keyValuePredicate.getKeyPredicate());
    }

    return this.translateStringPredicate(keyValuePredicate.getKeyPredicate())
        .flatMap(
            keyPredicate ->
                this.translateStringPredicate(keyValuePredicate.getValuePredicate())
                    .map(
                        valuePredicate ->
                            AttributePredicate.newBuilder()
                                .setNamePredicate(keyPredicate)
                                .setValuePredicate(valuePredicate)))
        .map(
            attributePredicate ->
                ConditionalProjector.Predicate.newBuilder()
                    .setAttributePredicate(attributePredicate)
                    .build());
  }
}
