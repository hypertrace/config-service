package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.URL_OR_QUERY_ATTRIBUTE_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.MatchConditionTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ParsedObjectKeyRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.RegexCaptureGroupProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.UrlEncodedProjector;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class QueryParamLocationTranslator implements RequestLocationTranslator {
  private final MatchConditionTranslator matchConditionTranslator;
  private static final String QUERY_PARAM_CAPTURE_REGEX = "\\?(.*)$";

  @Override
  public List<Projector> translateForRequest(
      MatchCondition matchCondition, AttributeRule attributeValue) {
    return URL_OR_QUERY_ATTRIBUTE_KEYS.stream()
        .map(location -> translate(matchCondition, attributeValue, location))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public RequestAttributeKeyLocation getRequestAttributeKeyLocation() {
    return RequestAttributeKeyLocation.REQUEST_ATTRIBUTE_KEY_LOCATION_QUERY_PARAMETER;
  }

  private Projector translate(
      MatchCondition matchCondition, AttributeRule attributeValue, String location) {
    return Projector.newBuilder()
        .setAttributeProjector(
            Projector.AttributeProjector.newBuilder()
                .setAttributeKey(location)
                .setAttributeRule(
                    AttributeRule.newBuilder()
                        .setProjector(
                            Projector.newBuilder()
                                .setRegexCaptureGroupProjector(
                                    RegexCaptureGroupProjector.newBuilder()
                                        .setRegexCaptureGroup(QUERY_PARAM_CAPTURE_REGEX)
                                        .setAttributeRule(
                                            AttributeRule.newBuilder()
                                                .setProjector(
                                                    Projector.newBuilder()
                                                        .setUrlEncodedProjector(
                                                            UrlEncodedProjector.newBuilder()
                                                                .setUrlParamRule(
                                                                    ParsedObjectKeyRule.newBuilder()
                                                                        .setKeyPredicate(
                                                                            matchConditionTranslator
                                                                                .translate(
                                                                                    matchCondition))
                                                                        .setAttributeRule(
                                                                            attributeValue)))))))))
        .build();
  }
}
