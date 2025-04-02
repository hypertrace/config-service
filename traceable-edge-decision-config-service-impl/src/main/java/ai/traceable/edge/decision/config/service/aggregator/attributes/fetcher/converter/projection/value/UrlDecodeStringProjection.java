package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class UrlDecodeStringProjection implements ValueProjection {
  private final boolean quotePlus;

  @Override
  public String apply(String inputJexl) {
    return String.format("traceableTransformUtils:urlDecode(%s, %b)", inputJexl, quotePlus);
  }
}
