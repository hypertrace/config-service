package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection;

import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value.ValueProjection;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.value.ValueProjectionFactory;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RootRelativeProjection implements Projection {
  private final List<ValueProjection> valueProjections;
  private final Function<String, String> rootTransformation;

  public RootRelativeProjection(
      ai.traceable.userattribution.config.service.v2.RootRelativeProjection rootRelativeProjection,
      Function<String, String> rootTransformation) {
    this.valueProjections =
        rootRelativeProjection.getValueProjectionsList().stream()
            .map(ValueProjectionFactory::create)
            .collect(Collectors.toUnmodifiableList());
    this.rootTransformation = rootTransformation;
  }

  @Override
  public String apply(String inputJexlExpression) {
    String rootTransformationJexl = rootTransformation.apply(inputJexlExpression);
    return valueProjections.stream()
        .reduce(
            rootTransformationJexl,
            (currentExpression, valueProjection) -> valueProjection.apply(currentExpression),
            (s1, s2) -> s1);
  }
}
