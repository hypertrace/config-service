package ai.traceable.jwt.extraction.config.service.converter.value;

public class JwtHeaderProjection implements ValueProjection {
  private static final String HTTP_REQUEST_HEADER_TRACEABLEAI_JWT_PREFIX =
      "http.request.header.traceableai_jwt_";
  private final String headerKey;

  public JwtHeaderProjection(String headerKey) {
    this.headerKey = headerKey;
  }

  @Override
  public String apply(String inputJexl) {
    return String.format(
        "traceableTransformUtils:extractJWTPath(%s, \"%s\", \"HEADER\")", inputJexl, headerKey);
  }

  @Override
  public String getKey() {
    return HTTP_REQUEST_HEADER_TRACEABLEAI_JWT_PREFIX + headerKey;
  }
}
