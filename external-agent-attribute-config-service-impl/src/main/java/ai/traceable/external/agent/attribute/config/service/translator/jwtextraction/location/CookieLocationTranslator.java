package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.COOKIE_HEADER_KEY;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import java.util.List;
import java.util.Optional;

public class CookieLocationTranslator extends AbstractLocationTranslator {

  @Override
  public List<LocationTranslationState> translateCase(JwtLocation location)
      throws JwtTranslationException {
    StringPredicate cookiePredicate = location.getRequestCookie();
    if (cookiePredicate.getOperator()
        != StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS) {
      throw new JwtTranslationException(
          "Only EQUALS predicate operator supported for cookie location");
    }
    return List.of(
        new LocationTranslationState(
            COOKIE_HEADER_KEY,
            Optional.of(cookiePredicate.getValue()),
            getRegexCaptureGroup(location),
            Optional.of(
                (attributeRule -> extractFromCookie(cookiePredicate.getValue(), attributeRule)))));
  }

  private AttributeRule extractFromCookie(String cookieName, AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            AttributeRule.Projector.newBuilder()
                .setCookieProjector(
                    AttributeRule.Projector.CookieProjector.newBuilder()
                        .setCookieNameRule(
                            AttributeRule.Projector.ParsedObjectKeyRule.newBuilder()
                                .setKey(cookieName)
                                .setAttributeRule(childRule))))
        .build();
  }
}
