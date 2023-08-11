package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.ratelimiting.service.v2.rules.modsec.datatype.DataTypeRuleWrapper;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataTypeRuleModsecConverter {
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
      List<Clause> ruleClauses = getRuleClauseLists(dataTypeRuleWrapper, environmentIds);

      return ruleClauses.stream()
          .map(
              clause ->
                  modsecBlobConverterUtils.convertToModsecRule(
                      dataTypeRuleWrapper.getModsecRuleId(),
                      wrappedUrlRegexesClause,
                      Collections.singletonList(clause),
                      String.format(
                          "Data type rule - %s %s",
                          dataTypeRuleWrapper.getDataTypeId(),
                          dataTypeRuleWrapper.getCustomLocation().hasCustomMatchingLocation()
                              ? "at custom location"
                              : ""),
                      String.format(
                          "Matched data-type - %s", dataTypeRuleWrapper.getRule().getName()),
                      modsecIdAssignment))
          .collect(Collectors.toUnmodifiableList());
    } catch (Exception e) {
      log.warn(
          "Cannot convert data-classification with details {} into modsec rule",
          dataTypeRuleWrapper,
          e);
      return Collections.emptyList();
    }
  }

  private List<Clause> getRuleClauseLists(
      DataTypeRuleWrapper dataTypeRuleWrapper, final List<String> environmentIds) {
    return dataTypeRuleWrapper.getRule().getScopedPatternsList().stream()
        .filter(scopedPattern -> filterScopedPattern(scopedPattern, environmentIds))
        .map(scopedPattern -> convertScopedPattern(dataTypeRuleWrapper, scopedPattern))
        .flatMap(List::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private static boolean filterScopedPattern(
      ScopedPattern scopedPattern, final List<String> environmentIds) {
    if (scopedPattern.getAction().equals(Action.ACTION_MATCH)) {
      if (scopedPattern.hasGlobalScope() || environmentIds.isEmpty()) {
        return true;
      }
      if (scopedPattern.hasEnvironmentScope()) {
        scopedPattern.getEnvironmentScope().getEnvironmentIdsList().retainAll(environmentIds);
      }
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
