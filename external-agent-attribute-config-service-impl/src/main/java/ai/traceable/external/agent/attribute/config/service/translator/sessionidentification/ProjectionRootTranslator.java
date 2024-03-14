package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.ResponseLocationTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.EachMatchingProjector;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.ProjectionRoot;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import ai.traceable.sessionidentification.config.service.v1.ValueProjection;
import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ProjectionRootTranslator {
  private final CustomProjectionTranslator customProjectionTranslator;
  private final AttributeProjectionTranslator attributeProjectionTranslator;
  private final ResponseLocationTranslatorLookup responseLocationTranslatorLookup;
  private final ValueProjectionsTranslator valueProjectionsTranslator;

  List<Projector> translateForTokenValue(
      SessionTokenRule tokenRule, RuleCreationSource ruleCreationSource) {
    ProjectionRoot projectionRoot = tokenRule.getTokenValueRule().getTokenValueProjection();
    if (projectionRoot.hasCustomProjection()) {
      return List.of(
          customProjectionTranslator.translateCustomProjection(
              projectionRoot.getCustomProjection()));
    }
    switch (tokenRule.getTokenTypeCase()) {
      case REQUEST_SESSION_TOKEN_DETAILS:
        return attributeProjectionTranslator.translateForRequest(tokenRule, ruleCreationSource);
      case RESPONSE_SESSION_TOKEN_DETAILS:
        return attributeProjectionTranslator.translateForResponse(tokenRule);
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Invalid Token type for session token rule %s", tokenRule))
            .asRuntimeException();
    }
  }

  List<Projector> translateForAttributeExpiration(
      ResponseSessionTokenDetails responseSessionTokenDetails) {
    ProjectionRoot projectionRoot =
        responseSessionTokenDetails.getResponseAttributeExpiration().getProjectionRoot();
    if (projectionRoot.hasCustomProjection()) {
      return List.of(
          customProjectionTranslator.translateCustomProjection(
              projectionRoot.getCustomProjection()));
    }

    return responseLocationTranslatorLookup
        .getTranslator(
            responseSessionTokenDetails.getResponseAttributeExpiration().getAttributeKeyLocation())
        .translateForResponse(
            projectionRoot.getAttributeProjection().getAttributeKeyMatchCondition(),
            valueProjectionsTranslator.translateValueProjections(
                projectionRoot.getAttributeProjection().getValueProjectionsInOrderList(),
                AttributeRule.newBuilder()));
  }

  List<Projector> translateForJwtExpiration(
      ResponseSessionTokenDetails responseSessionTokenDetails,
      AttributeProjection attributeProjection) {
    List<ValueProjection> valueProjections = attributeProjection.getValueProjectionsInOrderList();
    List<ValueProjection> valueProjectionsForJwtExpiration =
        valueProjections.stream()
            .filter(ValueProjection::hasJwtPayloadClaim)
            .findFirst()
            .map(
                valueProjection ->
                    valueProjections.subList(0, valueProjections.indexOf(valueProjection)))
            .orElse(valueProjections);

    return responseLocationTranslatorLookup
        .getTranslator(responseSessionTokenDetails.getTokenLocation())
        .translateForResponse(
            attributeProjection.getAttributeKeyMatchCondition(),
            valueProjectionsTranslator.translateValueProjections(
                valueProjectionsForJwtExpiration,
                AttributeRule.newBuilder()
                    .setProjector(
                        Projector.newBuilder()
                            .setJwtProjector(
                                Projector.JwtProjector.newBuilder()
                                    .setClaimRule(
                                        Projector.ParsedObjectKeyRule.newBuilder()
                                            .setKey("exp"))))));
  }

  private Projector buildProjector(List<Projector> projectors) {
    if (projectors.size() == 1) {
      return projectors.get(0);
    }
    return Projector.newBuilder()
        .setEachMatchingProjector(
            EachMatchingProjector.newBuilder()
                .addAllAttributeRules(
                    projectors.stream()
                        .map(
                            projector -> AttributeRule.newBuilder().setProjector(projector).build())
                        .collect(Collectors.toUnmodifiableList())))
        .build();
  }
}
