package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import java.util.List;
import java.util.Optional;

public abstract class AbstractLocationTranslator {
  public abstract List<LocationTranslationState> translateCase(JwtLocation location)
      throws JwtTranslationException;

  protected Optional<String> getRegexCaptureGroup(JwtLocation location) {
    if (location.hasRegexCaptureGroup()) {
      return Optional.of(location.getRegexCaptureGroup());
    }
    return Optional.empty();
  }
}
