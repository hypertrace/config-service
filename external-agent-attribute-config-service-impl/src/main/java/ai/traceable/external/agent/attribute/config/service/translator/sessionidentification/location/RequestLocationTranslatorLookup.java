package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RequestLocationTranslatorLookup {
  private final Map<RequestAttributeKeyLocation, RequestLocationTranslator> translatorMap;

  @Inject
  RequestLocationTranslatorLookup(Set<RequestLocationTranslator> translators) {
    this.translatorMap =
        translators.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    RequestLocationTranslator::getRequestAttributeKeyLocation,
                    Function.identity()));
  }

  public RequestLocationTranslator getTranslator(
      RequestAttributeKeyLocation requestAttributeKeyLocation) {
    if (!this.translatorMap.containsKey(requestAttributeKeyLocation)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "No translator defined for provided location: %s", requestAttributeKeyLocation))
          .asRuntimeException();
    }
    return this.translatorMap.get(requestAttributeKeyLocation);
  }
}
