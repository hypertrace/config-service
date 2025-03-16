package ai.traceable.jwt.extraction.config.service.converter.value;

public interface ValueProjection {
  String apply(String inputJexlExpression);

  default String getKey() {
    return "";
  }
}
