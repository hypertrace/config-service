package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import ai.traceable.auth.detection.config.service.v1.Predicate.PredicateCase;
import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class QueryAuthDetectionRuleTranslator extends AuthDetectionRuleTranslator {
  @Inject
  QueryAuthDetectionRuleTranslator(AttributeRuleBuilder attributeRuleBuilder) {
    super(attributeRuleBuilder);
  }

  @Override
  public PredicateCase getPredicateCase() {
    return PredicateCase.QUERY_PARAMETER_PREDICATE;
  }

  @Override
  Optional<Predicate> translatePredicate(
      ai.traceable.auth.detection.config.service.v1.Predicate predicate) {
    // TODO - we could do something like url match regex `.*?.*<key-regex>=<value-regex>.*`, but
    // negations would be an issue. Alternatively url encoded projector would only support equality
    log.error(
        "Query Parameter Auth Detection predicates not yet supported for agent evaluation: {}",
        predicate);
    return Optional.empty();
  }
}
