package ai.traceable.jwt.extraction.config.service.converter.value;

public class RegexCaptureGroupProjection implements ValueProjection {
  private final String regex;

  public RegexCaptureGroupProjection(String regex) {
    this.regex = regex;
  }

  @Override
  public String apply(String inputJexl) {
    return inputJexl + ".replaceAll(\"" + regex + "\", \"$1\")";
  }
}
