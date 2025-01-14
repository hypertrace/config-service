package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value;

public class ValueProjectionFactory {

  public static ValueProjection create(
      ai.traceable.userattribution.config.service.v2.ValueProjection valueProjection) {
    switch (valueProjection.getProjectionCase()) {
      case REGEX_CAPTURE_GROUP:
        return new RegexCaptureGroupProjection(valueProjection.getRegexCaptureGroup().getRegex());
      case JSON_PATH:
        return new JsonPathProjection(valueProjection.getJsonPath().getPath());
      case JWT_PAYLOAD_CLAIM:
        return new JwtClaimProjection(valueProjection.getJwtPayloadClaim().getClaimKey());
      case BASE64:
        return new Base64Projection();
      default:
        throw new IllegalArgumentException("Unsupported projection type");
    }
  }
}
