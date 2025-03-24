package ai.traceable.bot.categorized.config.service.v1.translator;

import static ai.traceable.bot.categorized.config.service.v1.MatchOperator.MATCH_OPERATOR_CONTAINS;
import static ai.traceable.bot.categorized.config.service.v1.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX;
import static ai.traceable.bot.categorized.config.service.v1.MatchOperator.MATCH_OPERATOR_NOT_CONTAINS;
import static ai.traceable.bot.categorized.config.service.v1.MatchOperator.MATCH_OPERATOR_NOT_MATCH_REGEX;

import ai.traceable.bot.categorized.config.service.v1.GenericMatchCondition;
import ai.traceable.bot.categorized.config.service.v1.KeyCondition;
import ai.traceable.bot.categorized.config.service.v1.KeyType;
import ai.traceable.bot.categorized.config.service.v1.MatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.AttributeDerivationMapping;
import ai.traceable.datamodel.data.transformation.config.v1.BinaryOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.FieldType;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.StructuredMatchCondition;
import java.util.Set;
import lombok.experimental.UtilityClass;

@UtilityClass
public class GenericMatchConditionToEdgeMatchConditionTranslator {

  private static final Set<MatchOperator> REGEX_OPERATORS =
      Set.of(
          MATCH_OPERATOR_CONTAINS,
          MATCH_OPERATOR_NOT_CONTAINS,
          MATCH_OPERATOR_MATCHES_REGEX,
          MATCH_OPERATOR_NOT_MATCH_REGEX);
  public static final String LHS = "lhs";
  public static final String JEXL_HEADER_EXTRACTOR_PATTERN = "request.headers['%s']";

  public static MatchCondition buildMatchCondition(
      final GenericMatchCondition genericMatchCondition) {
    final AttributeDerivationMapping lhs = getKeyAttributeDerivationMapping(genericMatchCondition);

    final BinaryOperator.Builder binaryOperator = BinaryOperator.newBuilder();
    binaryOperator.setMatchOperator(getMatchOperator(genericMatchCondition));

    final String valueString =
        genericMatchCondition.getValueMatchCondition().getMatchValue().getStringValue();
    if (REGEX_OPERATORS.contains(
        genericMatchCondition.getValueMatchCondition().getMatchOperator())) {
      binaryOperator.setRegex(valueString);
    } else {
      binaryOperator.setStringValue(valueString);
    }

    return buildMatchCondition(lhs, binaryOperator);
  }

  private ai.traceable.datamodel.data.transformation.config.v1.MatchOperator getMatchOperator(
      final GenericMatchCondition genericMatchCondition) {
    switch (genericMatchCondition.getValueMatchCondition().getMatchOperator()) {
      case MATCH_OPERATOR_EQUALS:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator.MATCH_OPERATOR_EQ;
      case MATCH_OPERATOR_NOT_EQUAL:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
            .MATCH_OPERATOR_NOT_EQ;

      case MATCH_OPERATOR_MATCHES_REGEX:
      case MATCH_OPERATOR_CONTAINS:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
            .MATCH_OPERATOR_LIKE;

      case MATCH_OPERATOR_NOT_MATCH_REGEX:
      case MATCH_OPERATOR_NOT_CONTAINS:
        return ai.traceable.datamodel.data.transformation.config.v1.MatchOperator
            .MATCH_OPERATOR_NOT_LIKE;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported match operator: %s",
                genericMatchCondition.getValueMatchCondition().getMatchOperator()));
    }
  }

  private AttributeDerivationMapping getKeyAttributeDerivationMapping(
      final GenericMatchCondition genericMatchCondition) {
    final KeyCondition keyCondition = genericMatchCondition.getKeyCondition();

    if (keyCondition.getKeyType() != KeyType.KEY_TYPE_HEADER) {
      throw new IllegalArgumentException(
          String.format("Unsupported key type: %s", keyCondition.getKeyType()));
    }

    if (keyCondition.getKeyMatchOperator().getMatchOperator()
        != MatchOperator.MATCH_OPERATOR_EQUALS) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported key match operator: %s",
              keyCondition.getKeyMatchOperator().getMatchOperator()));
    }

    final String headerName = keyCondition.getKeyMatchOperator().getMatchValue().getStringValue();
    return AttributeDerivationMapping.newBuilder()
        .setName(LHS)
        .setType(FieldType.FIELD_TYPE_STR)
        .addRules(
            DerivationRule.newBuilder()
                .setTransformationConfig(
                    DataTransformationConfig.newBuilder()
                        .setOutputType(FieldType.FIELD_TYPE_STR)
                        .setJexlExpression(
                            JexlExpressionConfig.newBuilder()
                                .setJexlExpression(
                                    String.format(
                                        JEXL_HEADER_EXTRACTOR_PATTERN, headerName.toLowerCase())))))
        .build();
  }

  private MatchCondition buildMatchCondition(
      final AttributeDerivationMapping lhs, final BinaryOperator.Builder binaryOperator) {
    return MatchCondition.newBuilder()
        .setStructuredMatchCondition(
            StructuredMatchCondition.newBuilder()
                .setLhs(lhs)
                .setBinaryOperator(binaryOperator)
                .build())
        .build();
  }
}
