package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_HEADER_KEY_FORMAT_STRINGS;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.RESPONSE_HEADER_KEY_FORMAT_STRINGS;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.MatchConditionTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.AttributeProjector;
import ai.traceable.sessionidentification.config.service.v1.LiteralValue;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.MatchOperator;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class HeaderLocationTranslator
    implements ResponseLocationTranslator, RequestLocationTranslator {
  private final MatchConditionTranslator matchConditionTranslator;
  private final CookieLocationTranslator cookieLocationTranslator;

  @Override
  public List<Projector> translateForRequest(
      MatchCondition matchCondition, AttributeRule attributeValue) {
    return REQUEST_HEADER_KEY_FORMAT_STRINGS.stream()
        .map(formatHeaderValue -> translate(matchCondition, attributeValue, formatHeaderValue))
        .collect(Collectors.toUnmodifiableList());
  }

  public List<Projector> translateForRequest(
      MatchCondition matchCondition,
      AttributeRule attributeValue,
      RuleCreationSource ruleCreationSource) {
    List<Projector> projectors = new ArrayList<>();
    // adding cookie projection for V1 migration case, as V1 previously handled the cookie case via
    // request header
    if (ruleCreationSource == RuleCreationSource.RULE_CREATION_SOURCE_OLD_API) {
      projectors.addAll(
          cookieLocationTranslator.translateForRequest(matchCondition, attributeValue));
    }
    projectors.addAll(this.translateForRequest(matchCondition, attributeValue));
    return projectors;
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
                    matchConditionTranslator.translate(addLocation(matchCondition, location)))
                .setAttributeRule(attributeValue))
        .build();
  }

  private MatchCondition addLocation(MatchCondition matchCondition, String location) {
    if (matchCondition.getMatchValue().getValueCase() != LiteralValue.ValueCase.STRING_VALUE) {
      return matchCondition;
    }
    String value = matchCondition.getMatchValue().getStringValue();
    if (matchCondition.getOperator() == MatchOperator.MATCH_OPERATOR_MATCHES_REGEX) {
      value = removeStartsWithIfPresent(value);
    } else if (matchCondition.getOperator() == MatchOperator.MATCH_OPERATOR_CONTAINS) {
      value = ".*" + value;
    }
    return MatchCondition.newBuilder(matchCondition)
        .setMatchValue(
            LiteralValue.newBuilder().setStringValue(String.format(location, value)).build())
        .build();
  }

  private String removeStartsWithIfPresent(String value) {

    if (value.startsWith("^")) {
      return value.substring(1);
    }
    return value;
  }
}
