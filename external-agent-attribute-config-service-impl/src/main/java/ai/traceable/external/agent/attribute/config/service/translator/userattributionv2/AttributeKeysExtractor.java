package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_HEADER_KEY_FORMAT_STRINGS;

import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AttributeKeysExtractor {

  List<String> getRequestHeaderAttributeKeys(String attributeName) {
    return REQUEST_HEADER_KEY_FORMAT_STRINGS.stream()
        .map(formatString -> String.format(formatString, attributeName.toLowerCase()))
        .collect(Collectors.toUnmodifiableList());
  }

  List<String> getResponseHeaderAttributeKeys(String attributeName) {
    return REQUEST_HEADER_KEY_FORMAT_STRINGS.stream()
        .map(formatString -> String.format(formatString, attributeName.toLowerCase()))
        .collect(Collectors.toUnmodifiableList());
  }
}
