package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import com.google.common.collect.Lists;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
class ValueProjectionsTranslator {
  private static final String JSON_PATH_PREFIX = "$.";
  private static final String JSON_ARRAY_PREFIX = "$[";
  private final AttributeRuleBuilder attributeRuleBuilder;

  AttributeRule translateValueProjections(
      List<ValueProjection> valueProjections, AttributeRule childAttributeRule) {
    List<ValueProjection> reversedValueProjections = Lists.reverse(valueProjections);
    AttributeRule prevAttributeRule = childAttributeRule;
    for (ValueProjection valueProjection : reversedValueProjections) {
      switch (valueProjection.getProjectionCase()) {
        case JSON_PATH:
          prevAttributeRule =
              attributeRuleBuilder.buildRuleForJsonPath(
                  getJsonPath(valueProjection.getJsonPath()), prevAttributeRule);
          break;
        case REGEX_CAPTURE_GROUP:
          prevAttributeRule =
              attributeRuleBuilder.buildRuleForRegexCaptureGroup(
                  valueProjection.getRegexCaptureGroup().getRegex(), prevAttributeRule);
          break;
        case BASE64:
          prevAttributeRule = attributeRuleBuilder.buildRuleForBase64(prevAttributeRule);
          break;
        case JWT_PAYLOAD_CLAIM:
          prevAttributeRule =
              attributeRuleBuilder.buildRuleForJwtClaim(
                  valueProjection.getJwtPayloadClaim().getClaimKey(), prevAttributeRule);
          break;
        case URL_DECODE_STRING:
          prevAttributeRule =
              attributeRuleBuilder.buildRuleForUrlEncodedString(
                  valueProjection.getUrlDecodeString().getQuotePlus(), prevAttributeRule);
          break;
        default:
          throw Status.INTERNAL
              .withDescription(
                  String.format(
                      "Unknown value projection case: %s", valueProjection.getProjectionCase()))
              .asRuntimeException();
      }
    }
    return prevAttributeRule;
  }

  private String getJsonPath(ValueProjection.JsonPathProjection jsonPath) {
    String path = jsonPath.getPath();
    return (path.startsWith(JSON_PATH_PREFIX) || path.startsWith(JSON_ARRAY_PREFIX))
        ? path
        : JSON_PATH_PREFIX + path;
  }
}
