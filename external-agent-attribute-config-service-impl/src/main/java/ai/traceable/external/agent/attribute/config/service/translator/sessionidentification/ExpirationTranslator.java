package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.AttributePredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.StringPredicate;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class ExpirationTranslator {
  private final ProjectionRootTranslator projectionRootTranslator;
  private final AttributeRuleBuilder attributeRuleBuilder;
  private final SessionIdentificationConstants sessionIdentificationConstants;

  AttributeRule translateExpiration(
      ResponseSessionTokenDetails responseSessionTokenDetails,
      int ruleIndex,
      String ruleId,
      AttributeProjection attributeProjection) {
    switch (responseSessionTokenDetails.getExpirationCase()) {
      case JWT_EXPIRATION:
        return addExpirationValueAttributeAndProject(
            projectionRootTranslator.translateForJwtExpiration(
                responseSessionTokenDetails, attributeProjection),
            sessionIdentificationConstants.buildKeyForExpirationValue(
                ruleId, ruleIndex, SessionIdentificationConstants.SESSION_NEW_PREFIX_KEY),
            sessionIdentificationConstants.buildKeyForNewSessionId(ruleId, ruleIndex));
      case RESPONSE_ATTRIBUTE_EXPIRATION:
        return addExpirationValueAttributeAndProject(
            projectionRootTranslator.translateForAttributeExpiration(responseSessionTokenDetails),
            sessionIdentificationConstants.buildKeyForExpirationValue(
                ruleId, ruleIndex, SessionIdentificationConstants.SESSION_NEW_PREFIX_KEY),
            sessionIdentificationConstants.buildKeyForNewSessionId(ruleId, ruleIndex));

      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid expiration type %s", responseSessionTokenDetails.getExpirationCase()))
            .asRuntimeException();
    }
  }

  AttributeRule translateExpiration(
      RequestSessionTokenDetails requestSessionTokenDetails,
      int ruleIndex,
      String ruleId,
      AttributeProjection attributeProjection) {
    switch (requestSessionTokenDetails.getExpirationCase()) {
      case JWT_EXPIRATION:
        return addExpirationValueAttributeAndProject(
            projectionRootTranslator.translateForJwtExpiration(
                requestSessionTokenDetails, attributeProjection),
            sessionIdentificationConstants.buildKeyForExpirationValue(
                ruleId, ruleIndex, SessionIdentificationConstants.SESSION_PREFIX_KEY),
            sessionIdentificationConstants.buildKeyForSessionId(ruleId, ruleIndex));
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format(
                    "Invalid expiration type %s", requestSessionTokenDetails.getExpirationCase()))
            .asRuntimeException();
    }
  }

  private AttributeRule addExpirationValueAttributeAndProject(
      List<Projector> projectors, String expirationKey, String sessionIdAttr) {
    return attributeRuleBuilder.buildFirstMatchingProjectorAttributeRule(
        projectors.stream()
            .map(
                projector ->
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
                                                                .setValue(sessionIdAttr))))
                                        .setAttributeRule(
                                            AttributeRule.newBuilder().setProjector(projector))))
                        .build())
            .collect(Collectors.toUnmodifiableList()),
        expirationKey);
  }
}
