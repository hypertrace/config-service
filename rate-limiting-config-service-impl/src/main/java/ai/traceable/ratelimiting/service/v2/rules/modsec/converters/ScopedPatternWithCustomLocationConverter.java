package ai.traceable.ratelimiting.service.v2.rules.modsec.converters;

import static ai.traceable.ratelimiting.service.v2.rules.modsec.converters.KeyRegexConverters.generateBlobRegexFromKeyPatterns;
import static ai.traceable.ratelimiting.service.v2.rules.modsec.converters.KeyRegexConverters.generateBlobRegexFromKeyValuePatterns;

import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.ratelimiting.config.service.v2.DatatypeCondition.RegexBasedMatching;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition;
import ai.traceable.ratelimiting.config.service.v2.KeyValueCondition.StringCondition;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScopedPatternWithCustomLocationConverter {
  private static final RateLimiter LOG_RATE_LIMITER = RateLimiter.create(0.01);
  private final ModsecBlobConverterUtils modsecBlobConverterUtils;

  @Inject
  public ScopedPatternWithCustomLocationConverter(
      ModsecBlobConverterUtils modsecBlobConverterUtils) {
    this.modsecBlobConverterUtils = modsecBlobConverterUtils;
  }

  List<Clause> convert(
      ScopedPattern scopedPattern, String dataTypeId, RegexBasedMatching customLocation) {
    try {
      return Collections.singletonList(buildCustomLocationClause(customLocation, scopedPattern));
    } catch (Exception e) {
      if (LOG_RATE_LIMITER.tryAcquire()) {
        log.warn(
            "Error while converting scopedPattern for dataTypeId: {} - {}. Skipping.",
            dataTypeId,
            scopedPattern);
      } else {
        log.debug(
            "Error while converting scopedPattern for dataTypeId: {} - {}. Skipping.",
            dataTypeId,
            scopedPattern,
            e);
      }
      return Collections.emptyList();
    }
  }

  private Clause buildCustomLocationClause(
      RegexBasedMatching customLocation, ScopedPattern scopedPattern) {
    KeyValueCondition keyValueCondition = customLocation.getCustomMatchingLocation();
    keyValueCondition =
        keyValueCondition.toBuilder()
            .setValueCondition(
                StringCondition.newBuilder()
                    .setValue(buildScopePatternCombinedRegex(scopedPattern))
                    .setOperator(KeyValueCondition.MatchOperator.MATCH_OPERATOR_MATCHES_REGEX))
            .build();

    return modsecBlobConverterUtils.buildKeyValueClause(keyValueCondition);
  }

  private String buildScopePatternCombinedRegex(ScopedPattern scopedPattern) {
    switch (scopedPattern.getPatternCase()) {
      case KEY_PATTERN:
        return generateBlobRegexFromKeyPatterns(scopedPattern.getKeyPattern().getValue());
      case LEAF_KEY_PATTERN:
        return generateBlobRegexFromKeyPatterns(scopedPattern.getLeafKeyPattern().getValue());
      case KEY_VALUE_PATTERN:
        return generateBlobRegexFromKeyValuePatterns(
            scopedPattern.getKeyValuePattern().getKeyPattern().getValue(),
            scopedPattern.getKeyValuePattern().getValuePattern().getValue());
      case LEAF_KEY_VALUE_PATTERN:
        return generateBlobRegexFromKeyValuePatterns(
            scopedPattern.getLeafKeyValuePattern().getKeyPattern().getValue(),
            scopedPattern.getLeafKeyValuePattern().getValuePattern().getValue());
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot convert datatype rule with scoped pattern %s", scopedPattern));
    }
  }
}
