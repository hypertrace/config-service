package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value;

class RegexCaptureGroupProjection implements ValueProjection {
  private final String regex;

  RegexCaptureGroupProjection(String regex) {
    this.regex = regex;
  }

  @Override
  public String apply(String inputJexl) {
    return inputJexl + ".replaceAll(\"" + regex + "\", \"$1\")";
  }
}
