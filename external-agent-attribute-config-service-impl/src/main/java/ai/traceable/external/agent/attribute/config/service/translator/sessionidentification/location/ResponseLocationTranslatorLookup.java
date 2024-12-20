package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeKeyLocation;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ResponseLocationTranslatorLookup {
  private final Map<ResponseAttributeKeyLocation, ResponseLocationTranslator> translatorMap;

  @Inject
  ResponseLocationTranslatorLookup(Set<ResponseLocationTranslator> translators) {
    this.translatorMap =
        translators.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    ResponseLocationTranslator::getResponseAttributeKeyLocation,
                    Function.identity()));
  }

  public ResponseLocationTranslator getTranslator(
      ResponseAttributeKeyLocation responseAttributeKeyLocation) {
    if (!this.translatorMap.containsKey(responseAttributeKeyLocation)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "No translator defined for provided location: %s", responseAttributeKeyLocation))
          .asRuntimeException();
    }
    return this.translatorMap.get(responseAttributeKeyLocation);
  }
}
