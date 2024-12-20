package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import ai.traceable.auth.detection.config.service.v1.Predicate.PredicateCase;
import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class FormBodyAuthDetectionRuleTranslator extends AuthDetectionRuleTranslator {
  @Inject
  FormBodyAuthDetectionRuleTranslator(AttributeRuleBuilder attributeRuleBuilder) {
    super(attributeRuleBuilder);
  }

  @Override
  public PredicateCase getPredicateCase() {
    return PredicateCase.FORM_BODY_PREDICATE;
  }

  @Override
  Optional<Predicate> translatePredicate(
      ai.traceable.auth.detection.config.service.v1.Predicate predicate) {
    // TODO could use capture group and content type?
    log.error("Form Body predicates not yet supported for agent evaluation: {}", predicate);
    return Optional.empty();
  }
}
