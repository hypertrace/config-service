package ai.traceable.data.parsing.config.service.v1.utils;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.data.parsing.config.service.v1.DataParsingConfig;

public class DataParsingConfigRankCalculator extends RankCalculator<DataParsingConfig, String> {

  public DataParsingConfigRankCalculator() {
    super(
        new RankConfig<>(
            DataParsingConfig::getRank,
            DataParsingConfig::getId,
            DataParsingConfigRankCalculator::rerankRule));
  }

  private static DataParsingConfig rerankRule(DataParsingConfig config, int rank) {
    return config.toBuilder().setRank(rank).build();
  }
}
