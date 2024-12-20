package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_COOKIE_HEADER_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.RESPONSE_COOKIE_HEADER_KEY;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.ParsedObjectKeyRuleTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.AttributeProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.CookieProjector;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeKeyLocation;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class CookieLocationTranslator
    implements ResponseLocationTranslator, RequestLocationTranslator {
  private final ParsedObjectKeyRuleTranslator parsedObjectKeyRuleTranslator;

  @Override
  public List<Projector> translateForRequest(
      MatchCondition matchCondition, AttributeRule attributeValue) {
    return List.of(translate(matchCondition, attributeValue, REQUEST_COOKIE_HEADER_KEY));
  }

  @Override
  public List<Projector> translateForResponse(
      MatchCondition matchCondition, AttributeRule attributeValue) {
    return List.of(translate(matchCondition, attributeValue, RESPONSE_COOKIE_HEADER_KEY));
  }

  @Override
  public RequestAttributeKeyLocation getRequestAttributeKeyLocation() {
    return RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_COOKIE;
  }

  @Override
  public ResponseAttributeKeyLocation getResponseAttributeKeyLocation() {
    return ResponseAttributeKeyLocation.RESPONSE_ATTRIBUTE_KEY_LOCATION_COOKIE;
  }

  private Projector translate(
      MatchCondition matchCondition, AttributeRule attributeValue, String location) {
    return Projector.newBuilder()
        .setAttributeProjector(
            AttributeProjector.newBuilder()
                .setAttributeKey(location)
                .setAttributeRule(
                    AttributeRule.newBuilder()
                        .setProjector(
                            Projector.newBuilder()
                                .setCookieProjector(
                                    CookieProjector.newBuilder()
                                        .setCookieNameRule(
                                            parsedObjectKeyRuleTranslator.translate(
                                                matchCondition, attributeValue))))))
        .build();
  }
}
