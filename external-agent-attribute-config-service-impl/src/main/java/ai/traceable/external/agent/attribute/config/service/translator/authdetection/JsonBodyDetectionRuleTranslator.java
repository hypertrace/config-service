package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_BODY_KEYS;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.auth.detection.config.service.v1.Predicate.PathPredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.PathValuePredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.PredicateCase;
import ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate;
import ai.traceable.auth.detection.config.service.v1.Predicate.StringPredicate.RelationalOperator;
import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Builder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class JsonBodyDetectionRuleTranslator extends AuthDetectionRuleTranslator {
  private static final String JSON_PATH_PREFIX = "$.";
  private final PredicateBuilder predicateBuilder;

  @Inject
  JsonBodyDetectionRuleTranslator(
      AttributeRuleBuilder attributeRuleBuilder, PredicateBuilder predicateBuilder) {
    super(attributeRuleBuilder);
    this.predicateBuilder = predicateBuilder;
  }

  @Override
  public PredicateCase getPredicateCase() {
    return PredicateCase.JSON_BODY_PREDICATE;
  }

  @Override
  public Stream<AttributeRule> translateRuleForAuthType(AuthDetectionRule rule) {
    return REQUEST_BODY_KEYS.stream()
        .map(
            attributeKey ->
                this.buildRule(
                    attributeKey,
                    rule.getPredicate().getJsonBodyPredicate(),
                    this.attributeRuleBuilder.buildActionAttributeRuleForAuthType(
                        rule.getAuthType(), rule.getId())))
        .flatMap(Optional::stream);
  }

  @Override
  Optional<Predicate> translatePredicate(
      ai.traceable.auth.detection.config.service.v1.Predicate predicate) {
    // TODO - we need to figure out the predicate vs attribute rule support
    log.error("Stand alone predicate not supported for json body matching");
    return Optional.empty();
  }

  private Optional<AttributeRule> buildRule(
      String attributeKey,
      PathValuePredicate pathValuePredicate,
      AttributeRule actionAttributeRule) {

    Optional<AttributeRule> childRuleOptional =
        pathValuePredicate.hasValuePredicate()
            ? this.buildValueConditionRule(
                pathValuePredicate.getValuePredicate(), actionAttributeRule)
            : Optional.of(actionAttributeRule);

    return this.buildJsonPath(pathValuePredicate.getPathPredicate())
        .flatMap(
            jsonPath ->
                childRuleOptional.map(
                    childRule ->
                        this.attributeRuleBuilder.buildRuleForAttribute(
                            attributeKey,
                            attributeRuleBuilder.buildRuleForJsonPath(jsonPath, childRule))));
  }

  private Optional<String> buildJsonPath(PathPredicate pathPredicate) {
    if (pathPredicate.getKeyPredicate().getOperator()
        != RelationalOperator.RELATIONAL_OPERATOR_EQUALS) {
      log.error("Unsupported path predicate - unsupported operator: {}", pathPredicate);
      return Optional.empty();
    }
    String jsonPathValue = pathPredicate.getKeyPredicate().getValue();

    if (jsonPathValue.isBlank()) {
      log.error("Unsupported path predicate - empty value: {}", pathPredicate);
      return Optional.empty();
    }

    return jsonPathValue.startsWith(JSON_PATH_PREFIX)
        ? Optional.of(jsonPathValue)
        : Optional.of(JSON_PATH_PREFIX + jsonPathValue);
  }

  private Optional<AttributeRule> buildValueConditionRule(
      StringPredicate valuePredicate, AttributeRule rule) {
    return this.predicateBuilder
        .translateCurrentValuePredicate(valuePredicate)
        .map(
            translatedPredicate ->
                AttributeRule.newBuilder()
                    .setProjector(
                        Projector.newBuilder()
                            .setConditionalProjector(
                                ConditionalProjector.newBuilder()
                                    .setPredicate(translatedPredicate)
                                    .setAttributeRule(rule))))
        .map(Builder::build);
  }
}
