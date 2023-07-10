package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_COOKIE_HEADER_KEY;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_HEADER_KEY_FORMAT_STRINGS;

import ai.traceable.userattribution.config.service.v1.UserAttributionRuleData.HeaderLocation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AttributeKeysExtractor {

  List<String> getHeaderAttributeKeys(HeaderLocation headerLocation) {
    if (!headerLocation.hasHeaderName()) {
      log.error("Header name absent in header location: {}", headerLocation);
      return Collections.emptyList();
    }
    return REQUEST_HEADER_KEY_FORMAT_STRINGS.stream()
        .map(
            formatString ->
                String.format(formatString, headerLocation.getHeaderName().toLowerCase()))
        .collect(Collectors.toUnmodifiableList());
  }

  Optional<List<String>> getHeaderAttributeKeysIfSet(HeaderLocation headerLocation) {
    return getStringIfSet(headerLocation.getHeaderName())
        .map(String::toLowerCase)
        .map(
            headerName ->
                REQUEST_HEADER_KEY_FORMAT_STRINGS.stream()
                    .map(formatString -> String.format(formatString, headerName))
                    .collect(Collectors.toUnmodifiableList()));
  }

  Optional<List<String>> getAttributeKeysIfSet(HeaderLocation headerLocation) {
    switch (headerLocation.getLocationCase()) {
      case HEADER_NAME:
        return getHeaderAttributeKeysIfSet(headerLocation);
      case COOKIE_NAME:
        return Optional.of(List.of(REQUEST_COOKIE_HEADER_KEY));
      case LOCATION_NOT_SET:
        return Optional.empty();
      default:
        log.error("Unrecognized header location: {}", headerLocation.getLocationCase());
        return Optional.empty();
    }
  }

  private Optional<String> getStringIfSet(String value) {
    return Optional.ofNullable(value).filter(Predicate.not(String::isEmpty));
  }
}
