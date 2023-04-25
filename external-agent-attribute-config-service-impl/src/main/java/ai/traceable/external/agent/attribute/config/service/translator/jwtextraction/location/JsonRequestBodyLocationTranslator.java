package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_BODY_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.PathPredicate;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class JsonRequestBodyLocationTranslator extends AbstractLocationTranslator {

  @Override
  public List<LocationTranslationState> translateCase(JwtLocation location)
      throws JwtTranslationException {
    PathPredicate jsonPathPredicate = location.getJsonRequestBody();
    if (jsonPathPredicate.getPathCase() != PathPredicate.PathCase.KEY_PREDICATE) {
      throw new JwtTranslationException("Only KEY predicate supported for path predicate");
    }
    StringPredicate jsonKeyPredicate = jsonPathPredicate.getKeyPredicate();
    if (jsonKeyPredicate.getOperator()
        != StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS) {
      throw new JwtTranslationException(
          "Only EQUALS predicate operator supported for json key location");
    }
    return REQUEST_BODY_KEYS.stream()
        .map(
            requestBodyAttributeKey ->
                new LocationTranslationState(
                    requestBodyAttributeKey,
                    Optional.of(jsonKeyPredicate.getValue()),
                    getRegexCaptureGroup(location),
                    Optional.of(
                        (attributeRule ->
                            extractFromJsonBody(jsonKeyPredicate.getValue(), attributeRule)))))
        .collect(Collectors.toUnmodifiableList());
  }

  private AttributeRule extractFromJsonBody(String jsonKey, AttributeRule childRule) {
    return AttributeRule.newBuilder()
        .setProjector(
            AttributeRule.Projector.newBuilder()
                .setJsonProjector(
                    AttributeRule.Projector.JsonProjector.newBuilder()
                        .setJsonPathRule(
                            AttributeRule.Projector.ParsedObjectKeyRule.newBuilder()
                                .setKey(jsonKey)
                                .setAttributeRule(childRule))))
        .build();
  }
}
