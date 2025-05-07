package ai.traceable.bot.categorized.config.service.v1.translator;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotCategoriesConfig;
import ai.traceable.bot.categorized.config.service.v1.CategorizedBotDetails;
import ai.traceable.bot.categorized.config.service.v1.CategorizedBotDetailsConfig;
import ai.traceable.bot.categorized.config.service.v1.LogicalMatchCondition;
import ai.traceable.bot.categorized.config.service.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.JexlExpressionConfig;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import java.util.List;
import java.util.stream.Collectors;

public class CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator {

  public static final CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator INSTANCE =
      new CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator();

  private CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator() {}

  public VariableDerivationMapping translate(final List<String> botIds) {
    final String jexlTransformExpression =
        CategorizedBotDetailsConfig.INSTANCE.getAllTraceableCategorizedBots().stream()
                .filter(bot -> botIds.contains(bot.getId()))
                .map(
                    tcBot -> {
                      final CategorizedBotDetails categorizedBotDetails =
                          tcBot.getCategorizedBotDetails();
                      final String matchExpression =
                          getTranslatedJexlExpression(
                              categorizedBotDetails
                                  .getCategorizedBotSignatureRule()
                                  .getMatchCondition());
                      return String.format(
                          "%s ? {'botId' : '%s', 'botName': '%s', 'botCategory' : '%s', 'botSubCategory' : '%s'} : ",
                          matchExpression,
                          tcBot.getId(),
                          categorizedBotDetails.getName(),
                          CategorizedBotCategoriesConfig.INSTANCE
                              .getBotCategory(categorizedBotDetails.getBotCategoryId())
                              .getBotCategoryName(),
                          CategorizedBotCategoriesConfig.INSTANCE
                              .getBotSubCategory(categorizedBotDetails.getBotSubCategoryId())
                              .getBotSubCategoryName());
                    })
                .collect(Collectors.joining())
            + " {} ";
    return VariableDerivationMapping.newBuilder()
        .setName("tcBot")
        .addRules(
            DerivationRule.newBuilder()
                .setTransformationConfig(
                    DataTransformationConfig.newBuilder()
                        .setJexlExpression(
                            JexlExpressionConfig.newBuilder()
                                .setJexlExpression(jexlTransformExpression)
                                .build())
                        .build())
                .build())
        .build();
  }

  private String getTranslatedJexlExpression(
      final ai.traceable.bot.categorized.config.service.v1.MatchCondition
          categorizedBotMatchCondition) {
    if (categorizedBotMatchCondition == null
        || categorizedBotMatchCondition.hasGenericMatchCondition()) {
      throw new IllegalArgumentException("Operation not supported");
    }
    String ipMatchExpression = "";
    String userAgentMatchExpression = "";
    if (categorizedBotMatchCondition.hasLogicalMatchCondition()) {
      final LogicalMatchCondition logicalMatchCondition =
          categorizedBotMatchCondition.getLogicalMatchCondition();
      final String leftExpression =
          getTranslatedJexlExpression(logicalMatchCondition.getLeftOperand());
      final String rightExpression =
          getTranslatedJexlExpression(logicalMatchCondition.getRightOperand());
      if (logicalMatchCondition.getOperator() == LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND) {
        return joinAnd(leftExpression, rightExpression);
      } else {
        return joinOr(leftExpression, rightExpression);
      }
    }
    if (categorizedBotMatchCondition.hasIpMetadataMatchCondition()) {
      ipMatchExpression =
          IpMetadataMatchConditionToJexlExpressionTranslator.buildJexlExpression(
              categorizedBotMatchCondition.getIpMetadataMatchCondition());
    }
    if (categorizedBotMatchCondition.hasUserAgentMatchCondition()) {
      userAgentMatchExpression =
          UserAgentMatchConditionToJexlExpressionTranslator.buildJexlExpression(
              categorizedBotMatchCondition.getUserAgentMatchCondition());
    }
    if (!ipMatchExpression.isEmpty() && !userAgentMatchExpression.isEmpty()) {
      return joinAnd(ipMatchExpression, userAgentMatchExpression);
    } else if (!ipMatchExpression.isEmpty()) {
      return ipMatchExpression;
    } else if (!userAgentMatchExpression.isEmpty()) {
      return userAgentMatchExpression;
    }
    throw new IllegalArgumentException("Unknown match condition type");
  }

  private static String joinOr(final String left, final String right) {
    return String.format("(%s) || (%s)", left, right);
  }

  private static String joinAnd(final String left, final String right) {
    return String.format("(%s) && (%s)", left, right);
  }
}
