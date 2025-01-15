package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher.converter.rule.TokenRuleConverter;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import java.util.Optional;

public class UserAttributionJexlGenerator {
  static final String JEXL_BASE_EXPRESSION = "$s";

  private final ScopeConverter scopeConverter = new ScopeConverter();

  DerivationRule convert(UserAttributionRuleData ruleData) {
    DerivationRule.Builder builder = DerivationRule.newBuilder();
    builder.setTransformationConfig(
        DataTransformationConfig.newBuilder()
            .setJexlExpression(
                JexlExpressionConfig.newBuilder()
                    .setJexlExpression(generateJexlExpression(ruleData)))
            .setOutputType(FieldType.FIELD_TYPE_STR));
    generateMatchCondition(ruleData).ifPresent(builder::setMatchCondition);
    return builder.build();
  }

  private String generateJexlExpression(UserAttributionRuleData ruleData) {
    return new TokenRuleConverter(ruleData.getUserIdRule(), ruleData.getRootTokenRule())
        .convert(JEXL_BASE_EXPRESSION);
  }

  // The jexl must be an if "condition", must eval to bool. if not, assume False.
  private Optional<MatchCondition> generateMatchCondition(UserAttributionRuleData ruleData) {
    return scopeConverter.convert(ruleData);
  }
}
