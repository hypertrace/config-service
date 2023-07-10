package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.Base64Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.JsonProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.JwtProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ParsedObjectKeyRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.RegexCaptureGroupProjector;
import ai.traceable.sessionidentification.config.service.v1.ValueProjection;
import com.google.common.collect.Lists;
import io.grpc.Status;
import java.util.List;

class ValueProjectionsTranslator {
  AttributeRule translateValueProjections(
      List<ValueProjection> valueProjections, AttributeRule.Builder matchConditionBuilder) {
    List<ValueProjection> reversedValueProjections = Lists.reverse(valueProjections);
    for (ValueProjection valueProjection : reversedValueProjections) {
      matchConditionBuilder = translateValueProjection(matchConditionBuilder, valueProjection);
    }
    return matchConditionBuilder.build();
  }

  private AttributeRule.Builder translateValueProjection(
      AttributeRule.Builder builder, ValueProjection valueProjection) {
    switch (valueProjection.getProjectionCase()) {
      case REGEX_CAPTURE_GROUP:
        return AttributeRule.newBuilder()
            .setProjector(
                Projector.newBuilder()
                    .setRegexCaptureGroupProjector(
                        RegexCaptureGroupProjector.newBuilder()
                            .setRegexCaptureGroup(
                                valueProjection.getRegexCaptureGroup().getRegexCaptureGroup())
                            .setAttributeRule(builder)));
      case JSON_PATH:
        return AttributeRule.newBuilder()
            .setProjector(
                Projector.newBuilder()
                    .setJsonProjector(
                        JsonProjector.newBuilder()
                            .setJsonPathRule(
                                ParsedObjectKeyRule.newBuilder()
                                    .setKey(valueProjection.getJsonPath().getPath())
                                    .setAttributeRule(builder))));
      case JWT_PAYLOAD_CLAIM:
        return AttributeRule.newBuilder()
            .setProjector(
                Projector.newBuilder()
                    .setJwtProjector(
                        JwtProjector.newBuilder()
                            .setClaimRule(
                                ParsedObjectKeyRule.newBuilder()
                                    .setKey(valueProjection.getJwtPayloadClaim().getClaimKey())
                                    .setAttributeRule(builder))));
      case BASE64_PROJECTION:
        return AttributeRule.newBuilder()
            .setProjector(
                Projector.newBuilder()
                    .setBase64Projector(Base64Projector.newBuilder().setAttributeRule(builder)));
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Invalid projection case %s", valueProjection))
            .asRuntimeException();
    }
  }
}
