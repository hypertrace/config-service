package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value;

public class Base64Projection implements ValueProjection {
  @Override
  public String apply(String inputJexl) {
    return String.format("traceableTransformUtils:base64Decode(%s)", inputJexl);
  }
}
