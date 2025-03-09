package ai.traceable.jwt.extraction.config.service.converter.value;

public class JwtHeaderProjection implements ValueProjection {
  private final String headerKey;

  public JwtHeaderProjection(String headerKey) {
    this.headerKey = headerKey;
  }

  @Override
  public String apply(String inputJexl) {
    return String.format(
        "traceableTransformUtils:extractJWTPath(%s, \"%s\", \"HEADER\")", inputJexl, headerKey);
  }
}
