package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import java.util.Optional;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** intermediary translated state pre merge with instructions */
@AllArgsConstructor
@Getter
public class LocationTranslationState {
  // for headers http.request.header.<header_name>, cookie/body it is
  // http.request.cookie/http.request.body
  private final String firstClassAttributeTag;
  // not stored for first class supported attributes like headers, but is stored for others
  private final Optional<String> keyUnderFirstClassAttribute;
  private final Optional<String> regexCaptureGroup;
  // used to perform custom transformation to parse the location before sending to regex / jwt
  // projector - ex: cookie
  private final Optional<Function<AttributeRule, AttributeRule>> locationCaptureRuleTransformation;
}
