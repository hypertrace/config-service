package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import ai.traceable.auth.detection.config.service.v1.Predicate.PredicateCase;
import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import java.util.Optional;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class CookieAuthDetectionRuleTranslator extends AuthDetectionRuleTranslator {
  @Inject
  CookieAuthDetectionRuleTranslator(AttributeRuleBuilder attributeRuleBuilder) {
    super(attributeRuleBuilder);
  }

  @Override
  public PredicateCase getPredicateCase() {
    return PredicateCase.COOKIE_PREDICATE;
  }

  @Override
  Optional<Predicate> translatePredicate(
      ai.traceable.auth.detection.config.service.v1.Predicate predicate) {
    // TODO - Maybe use a capture group projector to extract the appropriate cookie?
    //  Otherwise cookie projector only supports equality
    log.error(
        "Cookie Auth Detection predicates not yet supported for agent evaluation {}", predicate);
    return Optional.empty();
  }
}
