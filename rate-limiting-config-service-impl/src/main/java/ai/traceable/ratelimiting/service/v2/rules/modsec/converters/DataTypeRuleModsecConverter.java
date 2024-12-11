package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataTypeRuleWrapper;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataTypeRuleModsecConverter {
  private static final String DEFAULT_LOG_DATA_CAPTURE =
      "%{TX.0} found within %{MATCHED_VAR_NAME}: %{MATCHED_VAR}";
  private static final String DATA_TYPE_RULE_MESSAGE = "Data type rule - %s %s";
  private static final String DATA_TYPE_RULE_MESSAGE_CUSTOM_LOCATION_SUFFIX = "at custom location";
  private static final String DATA_TYPE_RULE_LOG_MESSAGE = "Matched data-type - %s as %s";

  private final ModsecBlobConverterUtils modsecBlobConverterUtils;
  private final ScopedPatternConverter scopedPatternConverter;
  private final ScopedPatternWithCustomLocationConverter scopedPatternWithCustomLocationConverter;

  @Inject
  public DataTypeRuleModsecConverter(
      ModsecBlobConverterUtils modsecBlobConverterUtils,
      ScopedPatternConverter scopedPatternConverter,
      ScopedPatternWithCustomLocationConverter scopedPatternWithCustomLocationConverter) {
    this.modsecBlobConverterUtils = modsecBlobConverterUtils;
    this.scopedPatternConverter = scopedPatternConverter;
    this.scopedPatternWithCustomLocationConverter = scopedPatternWithCustomLocationConverter;
  }

  public List<String> convertToModsecRule(
      DataTypeRuleWrapper dataTypeRuleWrapper,
      final List<String> wrapperUrlRegexes,
      final List<String> environmentIds,
      AtomicLong modsecIdAssignment) {
    try {
      Clause wrappedUrlRegexesClause =
          modsecBlobConverterUtils.buildUrlRegexClause(wrapperUrlRegexes);

      List<String> modsecBlobs = new ArrayList<>();
      for (int i = 0; i < dataTypeRuleWrapper.getRule().getScopedPatternsCount(); i++) {
        if (filterScopedPattern(
            dataTypeRuleWrapper.getRule().getScopedPatterns(i), environmentIds)) {
          String index = String.valueOf(i);
          convertScopedPattern(
                  dataTypeRuleWrapper, dataTypeRuleWrapper.getRule().getScopedPatterns(i))
              .stream()
              .map(
                  clause ->
                      buildModsecBlob(
                          dataTypeRuleWrapper,
                          modsecIdAssignment,
                          wrappedUrlRegexesClause,
                          index,
                          clause))
              .forEach(modsecBlobs::add);
        }
      }
      return modsecBlobs;
    } catch (Exception e) {
      log.warn(
          "Cannot convert data-classification with details {} into modsec rule",
          dataTypeRuleWrapper,
          e);
      return Collections.emptyList();
    }
  }

  private String buildModsecBlob(
      DataTypeRuleWrapper dataTypeRuleWrapper,
      AtomicLong modsecIdAssignment,
      Clause wrappedUrlRegexesClause,
      String index,
      Clause clause) {
    return modsecBlobConverterUtils.convertDataTypeToModsecRule(
        dataTypeRuleWrapper.getBaseModsecRuleId() + index,
        wrappedUrlRegexesClause,
        Collections.singletonList(clause),
        String.format(
            DATA_TYPE_RULE_MESSAGE,
            dataTypeRuleWrapper.getDataTypeId(),
            dataTypeRuleWrapper.getCustomLocation().hasCustomMatchingLocation()
                ? DATA_TYPE_RULE_MESSAGE_CUSTOM_LOCATION_SUFFIX
                : ""),
        String.format(
            DATA_TYPE_RULE_LOG_MESSAGE,
            dataTypeRuleWrapper.getRule().getName(),
            DEFAULT_LOG_DATA_CAPTURE),
        modsecIdAssignment);
  }

  private static boolean filterScopedPattern(
      ScopedPattern scopedPattern, final List<String> environmentIds) {
    if (scopedPattern.hasGlobalScope() || environmentIds.isEmpty()) {
      return true;
    }
    if (scopedPattern.hasEnvironmentScope()) {
      if (scopedPattern.getEnvironmentScope().getEnvironmentIdsList().isEmpty()) {
        return true;
      }
      return scopedPattern.getEnvironmentScope().getEnvironmentIdsList().stream()
          .anyMatch(environmentIds::contains);
    }
    return false;
  }

  private List<Clause> convertScopedPattern(
      DataTypeRuleWrapper dataTypeRuleWrapper, ScopedPattern scopedPattern) {
    if (dataTypeRuleWrapper.getCustomLocation().hasCustomMatchingLocation()) {
      return this.scopedPatternWithCustomLocationConverter.convert(
          scopedPattern,
          dataTypeRuleWrapper.getDataTypeId(),
          dataTypeRuleWrapper.getCustomLocation());
    } else {
      return this.scopedPatternConverter.convert(
          scopedPattern, dataTypeRuleWrapper.getDataTypeId());
    }
  }
}
