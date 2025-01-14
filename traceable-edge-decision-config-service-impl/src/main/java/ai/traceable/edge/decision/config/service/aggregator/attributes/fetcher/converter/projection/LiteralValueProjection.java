package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection;

import ai.traceable.userattribution.config.service.v2.LiteralValue;
import ai.traceable.userattribution.config.service.v2.LiteralValue.ValueCase;

public class LiteralValueProjection implements Projection {
  private final String literalValue;

  public LiteralValueProjection(LiteralValue literalValue) {
    this.literalValue =
        literalValue.getValueCase() == ValueCase.NULL_VALUE ? null : literalValue.getStringValue();
  }

  @Override
  public String apply(String inputJexlExpression) {
    return literalValue;
  }
}
