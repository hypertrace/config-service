package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.RegexBasedMatching;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataClassificationInfoProvider.DataClassificationInfo;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataTypeRuleWrapper;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataTypeConditionConverter {
  private static final String DELIMITER = ":";
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
                getRuleWrapper(
                    datatypeCondition.getDatatypeMatching().getRegexBasedMatching(),
                    dataTypeId,
                    dataClassificationInfo.getDataTypeRule(dataTypeId)))
        .collect(Collectors.toUnmodifiableList());
  }

  private static DataTypeRuleWrapper getRuleWrapper(
      RegexBasedMatching dataTypeMatching, String dataTypeId, DataTypeRule dataTypeRule) {
    return DataTypeRuleWrapper.builder()
        .modsecRuleId(DataTypeConditionConverter.generateModsecRuleId(dataTypeId, dataTypeMatching))
        .dataTypeId(dataTypeId)
        .customLocation(dataTypeMatching)
        .rule(dataTypeRule)
        .build();
  }

  /** Generate a modsec rule id based on combination of dataTypeId and custom location param */
  private static String generateModsecRuleId(
      String dataTypeId, RegexBasedMatching regexBasedMatching) {
    return String.join(DELIMITER, dataTypeId, uuidGenerator.generateId(regexBasedMatching));
  }
}
