package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_HEADER_KEY_FORMAT_STRINGS;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.StringPredicate;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RequestHeaderLocationTranslator extends AbstractLocationTranslator {
  @Override
  public List<LocationTranslationState> translateCase(JwtLocation location)
      throws JwtTranslationException {
    StringPredicate requestHeaderPredicate = location.getRequestHeader();
    if (requestHeaderPredicate.getOperator()
        != StringPredicate.RelationalOperator.RELATIONAL_OPERATOR_EQUALS) {
      throw new JwtTranslationException(
          "Only EQUALS predicate operator supported for header location");
    }
    return REQUEST_HEADER_KEY_FORMAT_STRINGS.stream()
        .map(formatString -> String.format(formatString, requestHeaderPredicate.getValue()))
        .map(
            qualifiedAttributeKey ->
                new LocationTranslationState(
                    qualifiedAttributeKey,
                    Optional.empty(),
                    getRegexCaptureGroup(location),
                    Optional.empty()))
        .collect(Collectors.toList());
  }
}
