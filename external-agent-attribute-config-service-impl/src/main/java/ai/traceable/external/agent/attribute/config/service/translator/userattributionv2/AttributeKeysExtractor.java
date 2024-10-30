package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import static ai.traceable.config.utils.RegexUtils.escapeRegex;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_HEADER_KEY_ESCAPED_FORMAT_STRINGS;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.REQUEST_HEADER_KEY_FORMAT_STRINGS;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.RESPONSE_HEADER_KEY_ESCAPED_FORMAT_STRINGS;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.RESPONSE_HEADER_KEY_FORMAT_STRINGS;
import static ai.traceable.userattribution.config.service.v2.KeyMatchOperator.KEY_MATCH_OPERATOR_EQUALS;

import ai.traceable.userattribution.config.service.v2.KeyMatch;
import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;

class AttributeKeysExtractor {

  List<String> getRequestHeaderAttributeKeys(KeyMatch keyMatch) {
    return getHeaderAttributeKeys(
        keyMatch, REQUEST_HEADER_KEY_FORMAT_STRINGS, REQUEST_HEADER_KEY_ESCAPED_FORMAT_STRINGS);
  }

  List<String> getResponseHeaderAttributeKeys(KeyMatch keyMatch) {
    return getHeaderAttributeKeys(
        keyMatch, RESPONSE_HEADER_KEY_FORMAT_STRINGS, RESPONSE_HEADER_KEY_ESCAPED_FORMAT_STRINGS);
  }

  private List<String> getHeaderAttributeKeys(
      KeyMatch keyMatch, List<String> keyFormatStrings, List<String> escapedKeyFormatStrings) {
    if (keyMatch.getOperator().equals(KEY_MATCH_OPERATOR_EQUALS)) {
      return keyFormatStrings.stream()
          .map(formatString -> String.format(formatString, keyMatch.getMatchKey().toLowerCase()))
          .collect(Collectors.toUnmodifiableList());
    } else {
      return escapedKeyFormatStrings.stream()
          .map(formatString -> String.format(formatString, translateSuffixKey(keyMatch)))
          .collect(Collectors.toUnmodifiableList());
    }
  }

  public String translateSuffixKey(KeyMatch keyMatch) {
    switch (keyMatch.getOperator()) {
      case KEY_MATCH_OPERATOR_CONTAINS:
        return ".*" + escapeRegex(keyMatch.getMatchKey());
      case KEY_MATCH_OPERATOR_STARTS_WITH:
        return escapeRegex(keyMatch.getMatchKey());
      case KEY_MATCH_OPERATOR_MATCHES_REGEX:
        return keyMatch.getMatchKey();
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Unable to convert match operator %s", keyMatch.getOperator()))
            .asRuntimeException();
    }
  }
}
