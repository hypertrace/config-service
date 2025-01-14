package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value;

public class JsonPathProjection implements ValueProjection {
  private final String path;

  public JsonPathProjection(String path) {
    this.path = path;
  }

  @Override
  public String apply(String inputJexl) {
    return String.format(
        "traceableTransformUtils:extractJsonPathValue(%s, \"%s\")", inputJexl, path);
  }
}
