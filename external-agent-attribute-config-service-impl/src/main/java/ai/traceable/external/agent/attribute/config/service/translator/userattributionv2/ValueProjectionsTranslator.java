package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.userattribution.config.service.v2.ValueProjection;
import com.google.common.collect.Lists;
import io.grpc.Status;
import java.util.List;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
class ValueProjectionsTranslator {
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
                  valueProjection.getJsonPath().getPath(), prevAttributeRule);
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
}
