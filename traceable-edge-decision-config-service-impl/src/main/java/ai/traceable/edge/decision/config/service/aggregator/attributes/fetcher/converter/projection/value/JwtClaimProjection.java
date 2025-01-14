package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value;

public class JwtClaimProjection implements ValueProjection {
  private final String claimKey;

  public JwtClaimProjection(String claimKey) {
    this.claimKey = claimKey;
  }

  @Override
  public String apply(String inputJexl) {
    return String.format(
        "traceableTransformUtils:extractJWTPath(%s, \"%s\", \"PAYLOAD\")", inputJexl, claimKey);
  }
}
