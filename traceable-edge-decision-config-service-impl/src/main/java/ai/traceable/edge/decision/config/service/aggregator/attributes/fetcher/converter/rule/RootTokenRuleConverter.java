package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.rule;

import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.projection.AttributeProjection;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;

class RootTokenRuleConverter implements BaseTokenRuleConverter {
  private static final UserAttributionRootTokenRule DEFAULT =
      UserAttributionRootTokenRule.getDefaultInstance();

  private final boolean isDefaultInstance;
  private final AttributeProjection attributeProjection;

  RootTokenRuleConverter(UserAttributionRootTokenRule rootTokenRule) {
    this.isDefaultInstance = rootTokenRule.equals(DEFAULT);
    this.attributeProjection =
        isDefaultInstance ? null : new AttributeProjection(rootTokenRule.getAttributeProjection());
  }

  @Override
  public String convert(String inputJexl) {
    if (isDefaultInstance) {
      return inputJexl;
    }
    return attributeProjection.apply(inputJexl);
  }
}
