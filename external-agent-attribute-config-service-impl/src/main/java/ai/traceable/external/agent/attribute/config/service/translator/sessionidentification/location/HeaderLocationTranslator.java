package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_HEADER_KEY_FORMAT_STRINGS;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.RESPONSE_HEADER_KEY_FORMAT_STRINGS;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.MatchConditionTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.AttributeProjector;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeKeyLocation;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class HeaderLocationTranslator
    implements ResponseLocationTranslator, RequestLocationTranslator {
  private final MatchConditionTranslator matchConditionTranslator;

  @Override
  public List<Projector> translateForRequest(
      MatchCondition matchCondition, AttributeRule attributeValue) {
    return REQUEST_HEADER_KEY_FORMAT_STRINGS.stream()
        .map(formatHeaderValue -> translate(matchCondition, attributeValue, formatHeaderValue))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<Projector> translateForResponse(
      MatchCondition matchCondition, AttributeRule attributeValue) {
    return RESPONSE_HEADER_KEY_FORMAT_STRINGS.stream()
        .map(formatHeaderValue -> translate(matchCondition, attributeValue, formatHeaderValue))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public RequestAttributeKeyLocation getRequestAttributeKeyLocation() {
    return RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_HEADER;
  }

  @Override
  public ResponseAttributeKeyLocation getResponseAttributeKeyLocation() {
    return ResponseAttributeKeyLocation.RESPONSE_ATTRIBUTE_KEY_LOCATION_HEADER;
  }

  private Projector translate(
      MatchCondition matchCondition, AttributeRule attributeValue, String location) {
    return Projector.newBuilder()
        .setAttributeProjector(
            AttributeProjector.newBuilder()
                .setAttributeKeyPredicate(
                    matchConditionTranslator.translateConditionWithValueTemplate(
                        matchCondition, location))
                .setAttributeRule(attributeValue))
        .build();
  }
}
