package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.RequestLocationTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.ResponseLocationTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.Predicate.AttributePredicate;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class AttributePredicateTranslator {
  private final ResponseLocationTranslatorLookup responseLocationTranslatorLookup;
  private final RequestLocationTranslatorLookup requestLocationTranslatorLookup;
  private final ValueProjectionsTranslator valueProjectionsTranslator;
  private final MatchConditionTranslator matchConditionTranslator;

  List<Projector> translate(AttributePredicate attributePredicate) {
    AttributeProjection attributeProjection = attributePredicate.getValueProjection();
    switch (attributePredicate.getAttributeKeyLocationCase()) {
      case REQUEST:
        return requestLocationTranslatorLookup
            .getTranslator(attributePredicate.getRequest())
            .translateForRequest(
                attributeProjection.getAttributeKeyMatchCondition(),
                valueProjectionsTranslator.translateValueProjections(
                    attributeProjection.getValueProjectionsInOrderList(),
                    addMatchConditionForPredicate(attributePredicate)),
                RuleCreationSource.RULE_CREATION_SOURCE_UNSPECIFIED);

      case RESPONSE:
        return responseLocationTranslatorLookup
            .getTranslator(attributePredicate.getResponse())
            .translateForResponse(
                attributeProjection.getAttributeKeyMatchCondition(),
                valueProjectionsTranslator.translateValueProjections(
                    attributeProjection.getValueProjectionsInOrderList(),
                    addMatchConditionForPredicate(attributePredicate)));
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid attribute key location present for attribute predicate %s",
                    attributePredicate))
            .asRuntimeException();
    }
  }

  private AttributeRule.Builder addMatchConditionForPredicate(
      AttributePredicate attributePredicate) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setConditionalProjector(
                    ConditionalProjector.newBuilder()
                        .setPredicate(
                            Predicate.newBuilder()
                                .setCurrentValuePredicate(
                                    matchConditionTranslator.translate(
                                        attributePredicate.getValueMatchCondition())))));
  }
}
