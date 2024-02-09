package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Action;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Action.AttributeAddition;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.AttributePredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.StringPredicate;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class ExpirationTranslator {
  private final ProjectionRootTranslator projectionRootTranslator;
  private final SessionIdentificationConstants sessionIdentificationConstants;

  List<Action> translateExpiration(
      ResponseSessionTokenDetails responseSessionTokenDetails,
      int ruleIndex,
      String ruleId,
      AttributeProjection attributeProjection) {
    switch (responseSessionTokenDetails.getExpirationCase()) {
      case JWT_EXPIRATION:
        return addExpirationValueAttributeAndProject(
            ruleId,
            ruleIndex,
            projectionRootTranslator.translateForJwtExpiration(
                responseSessionTokenDetails, attributeProjection));
      case RESPONSE_ATTRIBUTE_EXPIRATION:
        return addExpirationValueAttributeAndProject(
            ruleId,
            ruleIndex,
            projectionRootTranslator.translateForAttributeExpiration(responseSessionTokenDetails));

      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid expiration type %s", responseSessionTokenDetails.getExpirationCase()))
            .asRuntimeException();
    }
  }

  private List<Action> addExpirationValueAttributeAndProject(
      String ruleId, int ruleIndex, List<Projector> projectors) {
    return projectors.stream()
        .map(
            projector ->
                Action.newBuilder()
                    .setAttributeAddition(
                        AttributeAddition.newBuilder()
                            .setAttributeKey(
                                sessionIdentificationConstants.buildKeyForExpirationValue(
                                    ruleId, ruleIndex))
                            .setValueProjectionRule(
                                AttributeRule.newBuilder()
                                    .setProjector(
                                        Projector.newBuilder()
                                            .setConditionalProjector(
                                                ConditionalProjector.newBuilder()
                                                    .setPredicate(
                                                        Predicate.newBuilder()
                                                            .setAttributePredicate(
                                                                AttributePredicate.newBuilder()
                                                                    .setNamePredicate(
                                                                        StringPredicate.newBuilder()
                                                                            .setOperator(
                                                                                ComparisonOperator
                                                                                    .COMPARISON_OPERATOR_EQUALS)
                                                                            .setValue(
                                                                                sessionIdentificationConstants
                                                                                    .buildKeyForNewSessionId(
                                                                                        ruleId,
                                                                                        ruleIndex)))))
                                                    .setAttributeRule(
                                                        AttributeRule.newBuilder()
                                                            .setProjector(projector))))))
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }
}
