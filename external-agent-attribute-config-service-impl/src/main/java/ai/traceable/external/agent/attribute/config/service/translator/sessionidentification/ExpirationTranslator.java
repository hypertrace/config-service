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
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class ExpirationTranslator {
  private final ProjectionRootTranslator projectionRootTranslator;
  private final SessionIdentificationConstants sessionIdentificationConstants;

  AttributeRule translateExpiration(
      ResponseSessionTokenDetails responseSessionTokenDetails,
      int ruleIndex,
      String ruleId,
      MatchCondition matchCondition) {
    switch (responseSessionTokenDetails.getExpirationCase()) {
      case JWT_EXPIRATION:
        return AttributeRule.newBuilder()
            .addInitialActions(
                addExpirationValueAttributeAndProject(
                    ruleId,
                    ruleIndex,
                    projectionRootTranslator.translateForJwtExpiration(
                        responseSessionTokenDetails, matchCondition)))
            .build();
      case RESPONSE_ATTRIBUTE_EXPIRATION:
        return AttributeRule.newBuilder()
            .addInitialActions(
                addExpirationValueAttributeAndProject(
                    ruleId,
                    ruleIndex,
                    projectionRootTranslator.translateForAttributeExpiration(
                        responseSessionTokenDetails)))
            .build();

      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid expiration type %s", responseSessionTokenDetails.getExpirationCase()))
            .asRuntimeException();
    }
  }

  private Action addExpirationValueAttributeAndProject(
      String ruleId, int ruleIndex, Projector projector) {

    return Action.newBuilder()
        .setAttributeAddition(
            AttributeAddition.newBuilder()
                .setAttributeKey(
                    sessionIdentificationConstants.buildKeyForExpirationValue(ruleId, ruleIndex))
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
                                                                            ruleId, ruleIndex)))))
                                        .setAttributeRule(
                                            AttributeRule.newBuilder().setProjector(projector))))))
        .build();
  }
}
