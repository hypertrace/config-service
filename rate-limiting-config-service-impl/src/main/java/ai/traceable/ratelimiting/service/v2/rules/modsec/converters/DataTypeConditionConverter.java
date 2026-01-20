package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.RegexBasedMatching;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo.MatchCondition;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataClassificationInfoProvider.DataClassificationInfo;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataTypeRuleWrapper;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataTypeConditionConverter {
  private static final String DELIMITER = ":";
  private static final String PREFIX_TAIL = "_";
  private static final UuidGenerator uuidGenerator = new UuidGenerator();

  public static List<DataTypeRuleWrapper> converter(
      DatatypeCondition datatypeCondition, final DataClassificationInfo dataClassificationInfo) {
    return Stream.concat(
            datatypeCondition.getDatatypeIdsList().stream(),
            datatypeCondition.getDatasetIdsList().stream()
                .map(dataClassificationInfo::getDataTypeIdsForDataSet)
                .flatMap(List::stream))
        .distinct()
        .map(
            dataTypeId ->
                new AbstractMap.SimpleEntry<>(
                    dataTypeId, dataClassificationInfo.getDataTypeRule(dataTypeId)))
        .filter(entry -> entry.getValue() != null)
        .map(
            entry ->
                getRuleWrapper(
                    datatypeCondition.getDatatypeMatching().getRegexBasedMatching(),
                    entry.getKey(),
                    entry.getValue()))
        .collect(Collectors.toUnmodifiableList());
  }

  private static DataTypeRuleWrapper getRuleWrapper(
      RegexBasedMatching dataTypeMatching, String dataTypeId, DataTypeRule dataTypeRule) {
    String baseModsecRuleId = generateModsecRuleIdPrefix(dataTypeId, dataTypeMatching);
    return DataTypeRuleWrapper.builder()
        .baseModsecRuleId(baseModsecRuleId)
        .modsecMatchConditions(
            generateModsecMatchConditions(baseModsecRuleId, dataTypeRule.getScopedPatternsList()))
        .dataTypeId(dataTypeId)
        .customLocation(dataTypeMatching)
        .rule(dataTypeRule)
        .build();
  }

  /** Generate a modsec rule id based on combination of dataTypeId and custom location param */
  private static String generateModsecRuleIdPrefix(
      final String dataTypeId, final RegexBasedMatching regexBasedMatching) {
    return String.join(DELIMITER, dataTypeId, uuidGenerator.generateId(regexBasedMatching))
        + PREFIX_TAIL;
  }

  private static List<MatchCondition> generateModsecMatchConditions(
      final String baseId, List<ScopedPattern> scopedPatterns) {
    List<String> ignorePatternIds = new ArrayList<>();
    List<MatchCondition> matchConditions = new ArrayList<>();
    for (int i = 0; i < scopedPatterns.size(); i++) {
      String currentPatternId = baseId + i;
      if (scopedPatterns.get(i).getAction().equals(Action.ACTION_MATCH)) {
        matchConditions.add(
            MatchCondition.newBuilder()
                .setMatchId(currentPatternId)
                .addAllIgnoreIds(ignorePatternIds)
                .build());
      } else {
        ignorePatternIds.add(currentPatternId);
      }
    }
    return matchConditions;
  }
}
