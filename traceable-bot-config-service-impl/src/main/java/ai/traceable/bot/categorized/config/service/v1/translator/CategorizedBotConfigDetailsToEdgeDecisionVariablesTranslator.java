package ai.traceable.bot.categorized.config.service.v1.translator;

import static ai.traceable.datamodel.data.transformation.config.v1.FieldType.FIELD_TYPE_BOOL;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfig;
import ai.traceable.bot.categorized.config.service.v1.CategorizedBotDetailsConfig;
import ai.traceable.bot.categorized.utils.CategorizedBotStringUtil;
import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.LogicalMatchOperator;
import ai.traceable.datamodel.data.transformation.config.v1.MatchCondition;
import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import com.google.protobuf.Value;
import java.util.Collection;
import java.util.stream.Collectors;

public class CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator {

  public static final DataTransformationConfig BOT_VARIABLE_DATA_TRANSFORMATION_CONFIG =
      DataTransformationConfig.newBuilder()
          .setOutputType(FIELD_TYPE_BOOL)
          .setStaticValue(Value.newBuilder().setBoolValue(true).build())
          .build();
  public static final CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator INSTANCE =
      new CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator();

  private final Collection<VariableDerivationMapping> allTranslatedEdgeDecisionVariableDerivations;

  private CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator() {
    this.allTranslatedEdgeDecisionVariableDerivations =
        CategorizedBotDetailsConfig.INSTANCE.getAllTraceableCategorizedBots().stream()
            .map(this::convertBotConfigToVariableDerivation)
            .collect(Collectors.toUnmodifiableList());
  }

  public Collection<VariableDerivationMapping> translate() {
    return allTranslatedEdgeDecisionVariableDerivations;
  }

  private VariableDerivationMapping convertBotConfigToVariableDerivation(
      final CategorizedBotConfig categorizedBotConfig) {
    final String botName =
        CategorizedBotStringUtil.getFormattedBotName(
            categorizedBotConfig.getCategorizedBotDetails());
    return getVariableDerivationMapping(categorizedBotConfig, botName);
  }

  private VariableDerivationMapping getVariableDerivationMapping(
      final CategorizedBotConfig categorizedBotConfig, final String botName) {
    return VariableDerivationMapping.newBuilder()
        .setName(
            CategorizedBotStringUtil.getFormattedBotVariableName(
                botName, categorizedBotConfig.getId()))
        .addRules(getDerivationRule(categorizedBotConfig))
        .build();
  }

  private DerivationRule getDerivationRule(final CategorizedBotConfig categorizedBotConfig) {
    return DerivationRule.newBuilder()
        .setMatchCondition(getMatchCondition(categorizedBotConfig))
        .setTransformationConfig(BOT_VARIABLE_DATA_TRANSFORMATION_CONFIG)
        .build();
  }

  private MatchCondition getMatchCondition(final CategorizedBotConfig categorizedBotConfig) {
    return getTranslatedMatchCondition(
        categorizedBotConfig
            .getCategorizedBotDetails()
            .getCategorizedBotSignatureRule()
            .getMatchCondition());
  }

  private MatchCondition getTranslatedMatchCondition(
      final ai.traceable.bot.categorized.config.service.v1.MatchCondition
          categorizedBotMatchCondition) {
    if (categorizedBotMatchCondition.hasGenericMatchCondition()) {
      return GenericMatchConditionToEdgeMatchConditionTranslator.buildMatchCondition(
          categorizedBotMatchCondition.getGenericMatchCondition());
    }
    if (categorizedBotMatchCondition.hasIpMetadataMatchCondition()) {
      return IpMetadataMatchConditionToEdgeMatchConditionTranslator.buildMatchCondition(
          categorizedBotMatchCondition.getIpMetadataMatchCondition());
    }
    if (categorizedBotMatchCondition.hasUserAgentMatchCondition()) {
      return UserAgentMatchConditionToEdgeMatchConditionTranslator.buildMatchCondition(
          categorizedBotMatchCondition.getUserAgentMatchCondition());
    }
    if (categorizedBotMatchCondition.hasLogicalMatchCondition()) {
      final ai.traceable.bot.categorized.config.service.v1.LogicalMatchCondition
          logicalMatchCondition = categorizedBotMatchCondition.getLogicalMatchCondition();
      return MatchCondition.newBuilder()
          .setLogicalMatchCondition(
              LogicalMatchCondition.newBuilder()
                  .setOperator(getTranslatedOperator(logicalMatchCondition.getOperator()))
                  .addConditions(
                      getTranslatedMatchCondition(logicalMatchCondition.getLeftOperand()))
                  .addConditions(
                      getTranslatedMatchCondition(logicalMatchCondition.getRightOperand()))
                  .build())
          .build();
    } else {
      throw new IllegalArgumentException("Unknown match condition type");
    }
  }

  private LogicalMatchOperator getTranslatedOperator(
      final ai.traceable.bot.categorized.config.service.v1.LogicalMatchOperator operator) {
    switch (operator) {
      case LOGICAL_MATCH_OPERATOR_AND:
        return LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_AND;
      case LOGICAL_MATCH_OPERATOR_OR:
        return LogicalMatchOperator.LOGICAL_MATCH_OPERATOR_OR;
      default:
        throw new IllegalArgumentException("Unknown logical match operator: " + operator);
    }
  }
}
