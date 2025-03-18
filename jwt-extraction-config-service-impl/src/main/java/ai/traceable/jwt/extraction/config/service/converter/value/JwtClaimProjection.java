package ai.traceable.jwt.extraction.config.service.converter.value;

public class JwtClaimProjection implements ValueProjection {
  private static final String HTTP_REQUEST_BODY_TRACEABLEAI_JWT_PREFIX =
      "http.request.body.traceableai_jwt_";
  private final String claimKey;

  public JwtClaimProjection(String claimKey) {
    this.claimKey = claimKey;
  }

  @Override
  public String apply(String inputJexl) {
    return String.format(
        "traceableTransformUtils:extractJWTPath(%s, \"%s\", \"PAYLOAD\")", inputJexl, claimKey);
  }

  @Override
  public String getKey() {
    return HTTP_REQUEST_BODY_TRACEABLEAI_JWT_PREFIX + claimKey;
  }
}
