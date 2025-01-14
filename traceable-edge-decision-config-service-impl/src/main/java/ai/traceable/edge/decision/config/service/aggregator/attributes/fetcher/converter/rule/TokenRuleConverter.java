package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.rule;

import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.AttributeProjection;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.LiteralValueProjection;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.Projection;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.RootRelativeProjection;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;

public class TokenRuleConverter implements BaseTokenRuleConverter {
  private static final UserAttributionTokenRule DEFAULT =
      UserAttributionTokenRule.getDefaultInstance();

  private final boolean isDefaultInstance;
  private final Projection projection;

  public TokenRuleConverter(
      UserAttributionTokenRule tokenRule, UserAttributionRootTokenRule rootTokenRule) {
    this.isDefaultInstance = tokenRule.equals(DEFAULT);
    switch (tokenRule.getProjectionCase()) {
      case ATTRIBUTE_PROJECTION:
        this.projection = new AttributeProjection(tokenRule.getAttributeProjection());
        break;
      case LITERAL_VALUE_PROJECTION:
        this.projection =
            new LiteralValueProjection(tokenRule.getLiteralValueProjection().getLiteralValue());
        break;
      case ROOT_RELATIVE_PROJECTION:
        this.projection =
            new RootRelativeProjection(
                tokenRule.getRootRelativeProjection(),
                new RootTokenRuleConverter(rootTokenRule)::convert);
        break;
      case CUSTOM_PROJECTION:
      default:
        this.projection = null;
    }
  }

  @Override
  public String convert(String inputJexl) {
    if (isDefaultInstance) {
      return inputJexl;
    }

    // Apply the active projection strategy
    return projection.apply(inputJexl);
  }
}
