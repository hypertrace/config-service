package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import static ai.traceable.config.utils.RegexUtils.escapeRegex;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_HEADER_KEY_FORMAT_STRINGS;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.RESPONSE_HEADER_KEY_FORMAT_STRINGS;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.MatchConditionTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.AttributeProjector;
import ai.traceable.sessionidentification.config.service.v1.LiteralValue;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
                    addLocation(
                        matchCondition,
                        matchConditionTranslator.translate(matchCondition),
                        location))
                .setAttributeRule(attributeValue))
        .build();
  }

  private Projector.ConditionalProjector.Predicate.StringPredicate addLocation(
      MatchCondition matchCondition,
      Projector.ConditionalProjector.Predicate.StringPredicate predicate,
      String location) {
    if (matchCondition.getMatchValue().getValueCase() != LiteralValue.ValueCase.STRING_VALUE) {
      return predicate;
    }
    String escapedRegexLocation = escapeRegex(location.substring(0, location.indexOf("%s"))) + "%s";
    Projector.ConditionalProjector.Predicate.StringPredicate.Builder modifiedPredicate =
        predicate.toBuilder();
    switch (matchCondition.getOperator()) {
      case MATCH_OPERATOR_MATCHES_REGEX:
        return modifiedPredicate
            .setValue(
                String.format(
                    escapedRegexLocation, removeStartsWithIfPresent(predicate.getValue())))
            .build();
      case MATCH_OPERATOR_CONTAINS:
        return modifiedPredicate
            .setValue(String.format(escapedRegexLocation, ".*" + predicate.getValue()))
            .build();
      case MATCH_OPERATOR_STARTS_WITH:
        return modifiedPredicate
            .setValue(String.format(escapedRegexLocation, predicate.getValue()))
            .build();
      default:
        return modifiedPredicate.setValue(String.format(location, predicate.getValue())).build();
    }
  }

  private String removeStartsWithIfPresent(String value) {

    if (value.startsWith("^")) {
      return value.substring(1);
    }
    return value;
  }
}
