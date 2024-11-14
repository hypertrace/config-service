package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.RequestLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.RequestLocationTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.ResponseLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.ResponseLocationTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.CustomAttributeRule;
import ai.traceable.sessionidentification.config.service.v1.ProjectionRoot;
import ai.traceable.sessionidentification.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import ai.traceable.sessionidentification.config.service.v1.ValueProjection;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ProjectionRootTranslator {
  private static final String JWT_EXPIRATION_ATTR = "exp";
  private final CustomProjectionTranslator customProjectionTranslator;
  private final AttributeProjectionTranslator attributeProjectionTranslator;
  private final ResponseLocationTranslatorLookup responseLocationTranslatorLookup;
  private final RequestLocationTranslatorLookup requestLocationTranslatorLookup;
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
    return responseLocationTranslatorLookup
        .getTranslator(responseSessionTokenDetails.getTokenLocation())
        .translateForResponse(
            attributeProjection.getAttributeKeyMatchCondition(),
            addTranslationForJwtAttribute(
                attributeProjection.getValueProjectionsInOrderList(), JWT_EXPIRATION_ATTR));
  }

  List<Projector> translateForJwtExpiration(
      RequestSessionTokenDetails requestSessionTokenDetails,
      AttributeProjection attributeProjection) {
    return requestLocationTranslatorLookup
        .getTranslator(requestSessionTokenDetails.getTokenLocation())
        .translateForRequest(
            attributeProjection.getAttributeKeyMatchCondition(),
            addTranslationForJwtAttribute(
                attributeProjection.getValueProjectionsInOrderList(), JWT_EXPIRATION_ATTR));
  }

  Map<String, List<Projector>> translateForCustomAttribute(
      RequestSessionTokenDetails requestSessionTokenDetails,
      AttributeProjection attributeProjection) {
    RequestLocationTranslator locationTranslator =
        requestLocationTranslatorLookup.getTranslator(
            requestSessionTokenDetails.getTokenLocation());
    return requestSessionTokenDetails.getCustomAttributeRulesList().stream()
        .map(CustomAttributeRule::getJwtClaimAttributesFromSessionToken)
        .collect(
            Collectors.toMap(
                Function.identity(),
                jwtAttr ->
                    locationTranslator.translateForRequest(
                        attributeProjection.getAttributeKeyMatchCondition(),
                        addTranslationForJwtAttribute(
                            attributeProjection.getValueProjectionsInOrderList(), jwtAttr))));
  }

  Map<String, List<Projector>> translateForCustomAttribute(
      ResponseSessionTokenDetails responseSessionTokenDetails,
      AttributeProjection attributeProjection) {
    ResponseLocationTranslator locationTranslator =
        responseLocationTranslatorLookup.getTranslator(
            responseSessionTokenDetails.getTokenLocation());
    return responseSessionTokenDetails.getCustomAttributeRulesList().stream()
        .map(CustomAttributeRule::getJwtClaimAttributesFromSessionToken)
        .collect(
            Collectors.toMap(
                Function.identity(),
                jwtAttr ->
                    locationTranslator.translateForResponse(
                        attributeProjection.getAttributeKeyMatchCondition(),
                        addTranslationForJwtAttribute(
                            attributeProjection.getValueProjectionsInOrderList(), jwtAttr))));
  }

  private AttributeRule addTranslationForJwtAttribute(
      List<ValueProjection> valueProjections, String attributeKey) {
    List<ValueProjection> valueProjectionsForJwtAttribute =
        valueProjections.stream()
            .filter(ValueProjection::hasJwtPayloadClaim)
            .findFirst()
            .map(
                valueProjection ->
                    valueProjections.subList(0, valueProjections.indexOf(valueProjection)))
            .orElse(valueProjections);
    return valueProjectionsTranslator.translateValueProjections(
        valueProjectionsForJwtAttribute,
        AttributeRule.newBuilder()
            .setProjector(
                Projector.newBuilder()
                    .setJwtProjector(
                        Projector.JwtProjector.newBuilder()
                            .setClaimRule(
                                Projector.ParsedObjectKeyRule.newBuilder().setKey(attributeKey)))));
  }
}
