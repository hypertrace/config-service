package ai.traceable.data.handling.config.service.utils;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.data.handling.config.service.v1.DataHandlingRule;

public class DataHandlingRuleRankCalculator extends RankCalculator<DataHandlingRule, String> {

  public DataHandlingRuleRankCalculator() {
    super(
        new RankConfig<>(
            DataHandlingRule::getRank,
            DataHandlingRule::getId,
            DataHandlingRuleRankCalculator::rerankRule));
  }

  private static DataHandlingRule rerankRule(DataHandlingRule rule, int rank) {
    return rule.toBuilder().setRank(rank).build();
  }
}
